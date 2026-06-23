package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"

	clientpb "github.com/pit/client"
	"google.golang.org/grpc"
)

const (
	opcodeRead       = 24
	opcodeReadReply  = 25
	opcodeWrite      = 26
	opcodeWriteReply = 27
)

var (
	listenAddr = flag.String("addr", "localhost:50051", "address to listen on for ClientActor gRPC")
	gryffNodes = flag.String("gryff-nodes", "", "comma-separated gryff TCP addresses (host:port)")
	id         = flag.Int("id", 0, "index of this pit-client actor; offsets clientIds to avoid collisions across actors")
)

// gryffConn is a single TCP connection to one gryff replica. Ops are
// serialized via mu so we can read the reply inline without a dispatcher.
type gryffConn struct {
	mu        sync.Mutex
	r         *bufio.Reader
	w         *bufio.Writer
	clientId  int32
	requestId int32
}

func newGryffConn(addr string, clientId int32) (*gryffConn, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	r := bufio.NewReader(conn)
	w := bufio.NewWriter(conn)
	// genericsmr expects a 4-byte LE clientId as the first thing on each connection
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(clientId))
	if _, err := w.Write(hdr[:]); err != nil {
		return nil, fmt.Errorf("send clientId to %s: %w", addr, err)
	}
	if err := w.Flush(); err != nil {
		return nil, fmt.Errorf("flush clientId to %s: %w", addr, err)
	}
	return &gryffConn{r: r, w: w, clientId: clientId}, nil
}

// get sends a proxied Read and returns the value.
//
// Wire format (53 bytes): opcode(1) + Read(52)
// Read layout: [RequestId:4 LE][ClientId:4 LE][OId:8 LE][K:8 LE]
//              [Dep.Key:8 LE=-1][Dep.Vt.V:8 LE=0][Dep.Vt.T.Ts:4 LE=0]
//              [Dep.Vt.T.Cid:4 LE=0][Dep.Vt.T.Rmwc:4 LE=0]
//
// ReadReply (30 bytes): opcode(1) + [RequestId:4][ClientId:4][V:8][Ts:4][Cid:4][Rmwc:4][OK:1]
func (c *gryffConn) get(key int64, oid uint64) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	reqId := c.requestId
	c.requestId++

	var buf [53]byte
	buf[0] = opcodeRead
	b := buf[1:]
	binary.LittleEndian.PutUint32(b[0:4], uint32(reqId))
	binary.LittleEndian.PutUint32(b[4:8], uint32(c.clientId))
	binary.LittleEndian.PutUint64(b[8:16], oid)
	binary.LittleEndian.PutUint64(b[16:24], uint64(key))
	binary.LittleEndian.PutUint64(b[24:32], ^uint64(0)) // Dep.Key=-1 (empty dep)
	// Dep.Vt.V and all tag fields remain zero

	if _, err := c.w.Write(buf[:]); err != nil {
		return 0, fmt.Errorf("write Read: %w", err)
	}
	if err := c.w.Flush(); err != nil {
		return 0, fmt.Errorf("flush Read: %w", err)
	}

	var rep [30]byte
	if _, err := io.ReadFull(c.r, rep[:]); err != nil {
		return 0, fmt.Errorf("read ReadReply: %w", err)
	}
	if rep[0] != opcodeReadReply {
		return 0, fmt.Errorf("expected ReadReply opcode %d, got %d", opcodeReadReply, rep[0])
	}
	if rep[29] == 0 {
		return 0, fmt.Errorf("ReadReply.OK=0")
	}
	v := int64(binary.LittleEndian.Uint64(rep[9:17]))
	return v, nil
}

// put sends a proxied Write and waits for the WriteReply.
//
// Wire format (61 bytes): opcode(1) + Write(60)
// Write layout: [RequestId:4 LE][ClientId:4 LE][OId:8 LE][K:8 LE][V:8 LE]
//               [Dep.Key:8 LE=-1][Dep.Vt.V:8 LE=0][Dep.Vt.T.Ts:4 LE=0]
//               [Dep.Vt.T.Cid:4 LE=0][Dep.Vt.T.Rmwc:4 LE=0]
//
// WriteReply (22 bytes): opcode(1) + [RequestId:4][ClientId:4][Ts:4][Cid:4][Rmwc:4][OK:1]
func (c *gryffConn) put(key, value int64, oid uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reqId := c.requestId
	c.requestId++

	var buf [61]byte
	buf[0] = opcodeWrite
	b := buf[1:]
	binary.LittleEndian.PutUint32(b[0:4], uint32(reqId))
	binary.LittleEndian.PutUint32(b[4:8], uint32(c.clientId))
	binary.LittleEndian.PutUint64(b[8:16], oid)
	binary.LittleEndian.PutUint64(b[16:24], uint64(key))
	binary.LittleEndian.PutUint64(b[24:32], uint64(value))
	binary.LittleEndian.PutUint64(b[32:40], ^uint64(0)) // Dep.Key=-1 (empty dep)
	// Dep.Vt.V and all tag fields remain zero

	if _, err := c.w.Write(buf[:]); err != nil {
		return fmt.Errorf("write Write: %w", err)
	}
	if err := c.w.Flush(); err != nil {
		return fmt.Errorf("flush Write: %w", err)
	}

	var rep [22]byte
	if _, err := io.ReadFull(c.r, rep[:]); err != nil {
		return fmt.Errorf("read WriteReply: %w", err)
	}
	if rep[0] != opcodeWriteReply {
		return fmt.Errorf("expected WriteReply opcode %d, got %d", opcodeWriteReply, rep[0])
	}
	if rep[21] == 0 {
		return fmt.Errorf("WriteReply.OK=0")
	}
	return nil
}

type actor struct {
	clientpb.UnimplementedClientActorServer
	conns map[string]*gryffConn
}

func (a *actor) ExecuteOp(ctx context.Context, req *clientpb.OpRequest) (*clientpb.OpResponse, error) {
	conn, ok := a.conns[req.ServerAddr]
	if !ok {
		return nil, fmt.Errorf("no connection for server addr %q", req.ServerAddr)
	}

	switch req.OpType {
	case "get":
		key, err := strconv.ParseInt(req.Args[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse key %q: %w", req.Args[0], err)
		}
		v, err := conn.get(key, req.Oid)
		if err != nil {
			return nil, err
		}
		return &clientpb.OpResponse{Result: strconv.FormatInt(v, 10)}, nil

	case "put":
		key, err := strconv.ParseInt(req.Args[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse key %q: %w", req.Args[0], err)
		}
		value, err := strconv.ParseInt(req.Args[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse value %q: %w", req.Args[1], err)
		}
		if err := conn.put(key, value, req.Oid); err != nil {
			return nil, err
		}
		return &clientpb.OpResponse{Result: "true"}, nil

	default:
		return nil, fmt.Errorf("unknown op type: %q", req.OpType)
	}
}

func main() {
	flag.Parse()
	if *gryffNodes == "" {
		log.Fatal("-gryff-nodes is required")
	}

	conns := map[string]*gryffConn{}
	nodeList := strings.Split(*gryffNodes, ",")
	clientId := int32(*id * len(nodeList))
	for _, addr := range nodeList {
		addr = strings.TrimSpace(addr)
		c, err := newGryffConn(addr, clientId)
		if err != nil {
			log.Fatalf("connect to gryff node %s: %v", addr, err)
		}
		conns[addr] = c
		log.Printf("connected to gryff node %s (clientId=%d)", addr, clientId)
		clientId++
	}

	lis, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatalf("listen on %s: %v", *listenAddr, err)
	}
	s := grpc.NewServer()
	clientpb.RegisterClientActorServer(s, &actor{conns: conns})
	log.Printf("gryff-pit-client listening on %s", *listenAddr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
