package gryff

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
)

type pitVertex struct {
	vertexId  int64
	typeOnly  bool
	oid       uint64
	neighbors []int64
}

// PitSequencer manages PIT ordering for the gryff server. It listens on
// port+100 for an execution subgraph from the orchestrator, then ensures
// each Read/Write is dispatched only after its predecessors in the subgraph
// have completed.
type PitSequencer struct {
	mu           sync.Mutex
	predecessors map[uint64][]uint64      // oid → OIds that must finish first
	completed    map[uint64]struct{}      // OIds that have finished
	waiting      map[uint64][]chan struct{} // predecessor oid → channels to close on completion
	opToOid      map[int64]uint64         // (requestId<<32|clientId) → oid
}

func NewPitSequencer() *PitSequencer {
	return &PitSequencer{
		predecessors: make(map[uint64][]uint64),
		completed:    make(map[uint64]struct{}),
		waiting:      make(map[uint64][]chan struct{}),
		opToOid:      make(map[int64]uint64),
	}
}

// Start launches a goroutine that accepts one connection on port+100,
// reads the subgraph sent by the orchestrator, and populates the predecessor map.
func (ps *PitSequencer) Start(port int) {
	go func() {
		addr := net.JoinHostPort("0.0.0.0", strconv.Itoa(port+100))
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			fmt.Printf("pitseq listen: %v\n", err)
			return
		}
		fmt.Printf("pitseq: listening on %s\n", addr)
		conn, err := lis.Accept()
		if err != nil {
			fmt.Printf("pitseq accept: %v\n", err)
			return
		}
		defer conn.Close()
		fmt.Printf("pitseq: accepted connection\n")

		// Read buffer time (8 bytes, big-endian uint64 nanoseconds — unused)
		var bufNanos uint64
		if err := binary.Read(conn, binary.BigEndian, &bufNanos); err != nil {
			fmt.Printf("pitseq read bufNanos: %v\n", err)
			return
		}

		// Read protobuf-encoded Graph: 4-byte length then payload
		var length int32
		if err := binary.Read(conn, binary.BigEndian, &length); err != nil {
			fmt.Printf("pitseq read length: %v\n", err)
			return
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(conn, buf); err != nil {
			fmt.Printf("pitseq read graph: %v\n", err)
			return
		}

		vertices := parseProtoGraph(buf)
		ps.buildPredecessors(vertices)

		// Acknowledge receipt
		conn.Write([]byte{1})
		fmt.Printf("pitseq: received subgraph with %d vertices\n", len(vertices))
	}()
}

// Submit schedules fn once all predecessor OIds have completed.
// If all predecessors are already done (or there are none), fn is called
// immediately (from the caller's goroutine). Otherwise a goroutine is
// launched that waits for each outstanding predecessor then calls fn.
// The (requestId, clientId) → oid mapping is recorded for Complete().
func (ps *PitSequencer) Submit(oid uint64, requestId int32, clientId int32, fn func()) {
	ps.mu.Lock()
	key := (int64(requestId) << 32) | int64(clientId)
	ps.opToOid[key] = oid

	var waitChans []chan struct{}
	for _, pred := range ps.predecessors[oid] {
		if _, done := ps.completed[pred]; !done {
			ch := make(chan struct{})
			ps.waiting[pred] = append(ps.waiting[pred], ch)
			waitChans = append(waitChans, ch)
		}
	}
	ps.mu.Unlock()

	if len(waitChans) == 0 {
		fn()
		return
	}
	go func() {
		for _, ch := range waitChans {
			<-ch
		}
		fn()
	}()
}

// Complete marks the operation identified by (requestId, clientId) as done
// and unblocks any operations that were waiting on it.
func (ps *PitSequencer) Complete(requestId int32, clientId int32) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	key := (int64(requestId) << 32) | int64(clientId)
	oid, ok := ps.opToOid[key]
	if !ok {
		return
	}
	delete(ps.opToOid, key)
	ps.completed[oid] = struct{}{}

	for _, ch := range ps.waiting[oid] {
		close(ch)
	}
	delete(ps.waiting, oid)
}

// buildPredecessors inverts the graph edges (v → neighbor means neighbor
// depends on v) to build a predecessor map keyed by OId.
func (ps *PitSequencer) buildPredecessors(vertices []pitVertex) {
	// Map vertex_id → oid for non-type_only vertices
	vidToOid := make(map[int64]uint64)
	for _, v := range vertices {
		if !v.typeOnly {
			vidToOid[v.vertexId] = v.oid
		}
	}
	// For edge v → nb: nb must wait for v to complete
	for _, v := range vertices {
		if v.typeOnly {
			continue
		}
		for _, nbId := range v.neighbors {
			nbOid, ok := vidToOid[nbId]
			if !ok {
				continue
			}
			ps.predecessors[nbOid] = append(ps.predecessors[nbOid], v.oid)
		}
	}
}

// --- Minimal inline protobuf decoder for graph.proto Graph/Vertex messages ---
//
// Graph  { repeated Vertex vertices = 1; }
// Vertex { int64 vertex_id = 1; bool type_only = 2; uint64 oid = 3; repeated int64 neighbors = 4; }

func decodeVarint(b []byte) (uint64, int) {
	var x uint64
	var s uint
	for i, c := range b {
		if c < 0x80 {
			return x | uint64(c)<<s, i + 1
		}
		x |= uint64(c&0x7f) << s
		s += 7
	}
	return 0, 0
}

func parseProtoGraph(buf []byte) []pitVertex {
	var vertices []pitVertex
	i := 0
	for i < len(buf) {
		tag, n := decodeVarint(buf[i:])
		i += n
		if n == 0 {
			break
		}
		fieldNum := tag >> 3
		wireType := tag & 0x7
		switch {
		case fieldNum == 1 && wireType == 2: // vertices (LEN)
			length, n := decodeVarint(buf[i:])
			i += n
			v := parseProtoVertex(buf[i : i+int(length)])
			i += int(length)
			vertices = append(vertices, v)
		case wireType == 2: // skip unknown LEN field
			length, n := decodeVarint(buf[i:])
			i += n + int(length)
		case wireType == 0: // skip unknown VARINT field
			_, n := decodeVarint(buf[i:])
			i += n
		default:
			i = len(buf) // bail on unknown wire types
		}
	}
	return vertices
}

func parseProtoVertex(buf []byte) pitVertex {
	var v pitVertex
	i := 0
	for i < len(buf) {
		tag, n := decodeVarint(buf[i:])
		i += n
		if n == 0 {
			break
		}
		fieldNum := tag >> 3
		wireType := tag & 0x7
		switch {
		case fieldNum == 1 && wireType == 0: // vertex_id int64
			val, n := decodeVarint(buf[i:])
			i += n
			v.vertexId = int64(val)
		case fieldNum == 2 && wireType == 0: // type_only bool
			val, n := decodeVarint(buf[i:])
			i += n
			v.typeOnly = val != 0
		case fieldNum == 3 && wireType == 0: // oid uint64
			val, n := decodeVarint(buf[i:])
			i += n
			v.oid = val
		case fieldNum == 4 && wireType == 2: // neighbors packed repeated int64
			length, n := decodeVarint(buf[i:])
			i += n
			end := i + int(length)
			for i < end {
				val, n := decodeVarint(buf[i:])
				i += n
				v.neighbors = append(v.neighbors, int64(val))
			}
		case fieldNum == 4 && wireType == 0: // neighbors unpacked int64
			val, n := decodeVarint(buf[i:])
			i += n
			v.neighbors = append(v.neighbors, int64(val))
		case wireType == 2: // skip unknown LEN field
			length, n := decodeVarint(buf[i:])
			i += n + int(length)
		case wireType == 0: // skip unknown VARINT field
			_, n := decodeVarint(buf[i:])
			i += n
		default:
			i = len(buf)
		}
	}
	return v
}
