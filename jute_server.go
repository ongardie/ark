package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
)

type InfiniteQueue struct {
	mutex  sync.Mutex
	queue  [][]byte
	signal chan struct{}
}

func NewInfiniteQueue() *InfiniteQueue {
	return &InfiniteQueue{
		signal: make(chan struct{}),
	}
}

func (q *InfiniteQueue) Push(msg []byte) {
	q.mutex.Lock()
	q.queue = append(q.queue, msg)
	q.mutex.Unlock()
	select {
	case q.signal <- struct{}{}:
	default:
	}
}

func (q *InfiniteQueue) Pop() []byte {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if len(q.queue) > 0 {
		first := q.queue[0]
		q.queue = q.queue[1:]
		return first
	}
	return nil
}

type Received struct {
	conn      *JuteConnection
	msg       []byte
	isConnReq bool
}

type JuteConnection struct {
	appConn   Connection
	netConn   net.Conn
	received  chan<- Received
	sendQueue *InfiniteQueue
	closeCh   chan struct{}
}

func (conn *JuteConnection) handshake() {
	// Receive connection request from the client
	req, err := conn.receive()
	if err != nil {
		log.Printf("Error receiving connection request (%v), closing connection", err)
		conn.close()
		return
	}

	// Queue it for processing
	select {
	case conn.received <- Received{conn: conn, msg: req, isConnReq: true}:
	case <-conn.closeCh:
		conn.closeCh <- struct{}{}
		return
	}

	// Wait for the response
	msg := conn.toSend()
	if msg == nil {
		return
	}

	// Send it back to the client
	err = conn.send(msg)
	if err != nil {
		log.Printf("Error sending connection response (%v), closing connection", err)
		conn.close()
		return
	}

	// Start up the send and receive routines
	log.Printf("Done setting up new connection")
	go conn.sendLoop()
	go conn.receiveLoop()
}

func encode(msg interface{}) ([]byte, error) {
	bufSize := 1024
	for {
		buf := make([]byte, bufSize)
		n, err := encodePacket(buf, msg)
		if err == nil {
			return buf[:n], nil
		}
		if err == ErrShortBuffer {
			log.Printf("buffer size of %v too small, doubling", bufSize)
			bufSize *= 2
			continue
		}
		return nil, err
	}
}

// It's safe to call close() more than once. To make this work, we can't simply
// close(closeCh).
func (conn *JuteConnection) close() {
	select {
	case conn.closeCh <- struct{}{}:
	default:
	}
	err := conn.netConn.Close()
	if err != nil {
		log.Printf("Closing connection returned error %v", err)
	}
}

func (conn *JuteConnection) receive() ([]byte, error) {
	log.Printf("Waiting for incoming message")
	buf := make([]byte, 4)
	n, err := io.ReadFull(conn.netConn, buf)
	if err != nil {
		return nil, err
	}
	bytes := binary.BigEndian.Uint32(buf[:n])
	log.Printf("Expecting %v bytes", bytes)
	buf = make([]byte, bytes)
	_, err = io.ReadFull(conn.netConn, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (conn *JuteConnection) receiveLoop() {
	for {
		req, err := conn.receive()
		if err != nil {
			log.Printf("Error receiving message (%v), closing connection", err)
			conn.close()
			return
		}
		select {
		case conn.received <- Received{conn: conn, msg: req, isConnReq: false}:
		case <-conn.closeCh:
			conn.closeCh <- struct{}{}
			return
		}
	}
}

func (conn *JuteConnection) send(msg []byte) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(len(msg)))
	buf = append(buf, msg...)
	_, err := conn.netConn.Write(buf)
	return err
}

func (conn *JuteConnection) toSend() []byte {
	msg := conn.sendQueue.Pop()
	for msg == nil {
		select {
		case <-conn.sendQueue.signal:
		case <-conn.closeCh:
			conn.closeCh <- struct{}{}
			return nil
		}
		msg = conn.sendQueue.Pop()
	}
	return msg
}

func (conn *JuteConnection) sendLoop() {
	for {
		msg := conn.toSend()
		if msg == nil {
			return
		}
		err := conn.send(msg)
		if err != nil {
			log.Printf("Error sending message (%v), closing connection", err)
			conn.close()
			return
		}
	}
}

type JuteServer struct {
	received chan Received
	rpcChan  chan<- RPC
}

func NewJuteServer(rpcChan chan RPC) *JuteServer {
	s := &JuteServer{
		received: make(chan Received),
		rpcChan:  rpcChan,
	}
	go s.receive()
	return s
}

func (s *JuteServer) Listen(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("Error listening on %v: %v", addr, err)
	}
	go s.acceptLoop(listener)
	return nil
}

func (s *JuteServer) acceptLoop(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err == nil {
			s.newConnection(conn)
		} else {
			log.Printf("Error from Accept, ignoring: %v", err)
		}
	}
}

func (s *JuteServer) newConnection(netConn net.Conn) {
	conn := &JuteConnection{
		netConn:   netConn,
		received:  s.received,
		sendQueue: NewInfiniteQueue(),
		closeCh:   make(chan struct{}, 1),
	}
	go conn.handshake()
}

func decode(buf []byte, msg interface{}) ([]byte, error) {
	n, err := decodePacket(buf, msg)
	if err != nil {
		return nil, err
	}
	buf = buf[n:]
	log.Printf("DecodePacket returned %v, so %v bytes left", n, len(buf))
	return buf, nil
}

func (s *JuteServer) processConnReq(received Received) error {
	req := &connectRequest{}
	buf, err := decode(received.msg, req)
	if err != nil {
		return fmt.Errorf("error reading ConnectRequest: %v", err)
	}
	sendReadOnlyByte := false
	if len(buf) == 1 {
		// Modern ZK clients send 1 more byte to indicate whether they support
		// read-only mode. We ignore it but send a 0 byte back.
		sendReadOnlyByte = true
	} else if len(buf) > 1 {
		return fmt.Errorf("unexpected bytes after ConnectRequest: %#v", buf)
	}
	log.Printf("connection request: %#v", req)
	s.rpcChan <- &ConnectRPC{
		conn: &received.conn.appConn,
		req:  req,
		errReply_: func(errCode ErrCode) {
			// TODO: what am I supposed to do with this?
			log.Printf("Can't satisfy connection request (%v), dropping", errCode.toError())
			received.conn.close()
		},
		reply: func(resp *connectResponse) {
			log.Printf("Replying to connection request with %#v", resp)
			buf, err = encode(resp)
			if err != nil {
				log.Printf("Error serializing ConnectResponse: %v", err)
				received.conn.close()
				return
			}
			if sendReadOnlyByte {
				buf = append(buf, 0)
			}
			received.conn.sendQueue.Push(buf)
		},
	}
	return nil
}

func (s *JuteServer) process(received Received) error {
	if received.isConnReq {
		return s.processConnReq(received)
	}
	base := baseRPC{
		conn: &received.conn.appConn,
	}
	more, err := decode(received.msg, &base.reqHeader)
	if err != nil {
		return fmt.Errorf("error reading request header: %v", err)
	}
	name, ok := opNames[base.reqHeader.Opcode]
	if !ok {
		return fmt.Errorf("unknown opcode: %#v", base.reqHeader)
	}

	base.respHeader.Xid = base.reqHeader.Xid
	base.respHeader.Zxid = 1 // TODO
	base.respHeader.Err = errOk

	base.errReply_ = func(errCode ErrCode) {
		base.respHeader.Err = errCode
		log.Printf("Replying with error header to %v: %+v", name, base.respHeader)
		buf, err := encode(&base.respHeader)
		if err != nil {
			log.Printf("Error encoding response header to %v: %v", name, err)
			received.conn.close()
			return
		}
		received.conn.sendQueue.Push(buf)
	}

	normalReply := func(msg interface{}) {
		headerBuf, err := encode(&base.respHeader)
		if err != nil {
			log.Printf("Error encoding response header to %v: %v", name, err)
			received.conn.close()
			return
		}
		msgBuf, err := encode(msg)
		if err != nil {
			log.Printf("Error encoding response to %v: %v", name, err)
			received.conn.close()
			return
		}
		received.conn.sendQueue.Push(append(headerBuf, msgBuf...))
	}

	decodeRemaining := func(msg interface{}) error {
		more, err = decode(more, msg)
		if err != nil {
			return fmt.Errorf("error reading %v request: %v", name, err)
		}
		log.Printf("Read: %#v", msg)
		if len(more) > 0 {
			log.Printf("unexpected bytes after reading %v request: %v", name, more)
		}
		return nil
	}

	var rpc RPC
	switch base.reqHeader.Opcode {

	case opCreate:
		req := &CreateRequest{}
		rpc = &CreateRPC{
			baseRPC: base,
			req:     req,
			reply:   func(resp *createResponse) { normalReply(resp) },
		}
		err = decodeRemaining(req)

	case opGetChildren2:
		req := &getChildren2Request{}
		rpc = &GetChildrenRPC{
			baseRPC: base,
			req:     req,
			reply:   func(resp *getChildren2Response) { normalReply(resp) },
		}
		err = decodeRemaining(req)

	case opGetData:
		req := &getDataRequest{}
		rpc = &GetDataRPC{
			baseRPC: base,
			req:     req,
			reply:   func(resp *getDataResponse) { normalReply(resp) },
		}
		err = decodeRemaining(req)

	case opSetData:
		req := &SetDataRequest{}
		rpc = &SetDataRPC{
			baseRPC: base,
			req:     req,
			reply:   func(resp *setDataResponse) { normalReply(resp) },
		}
		err = decodeRemaining(req)

	default:
		return fmt.Errorf("Unknown request")
	}

	if err != nil {
		return err
	}
	s.rpcChan <- rpc
	return nil
}

func (s *JuteServer) receive() {
	for {
		received := <-s.received
		err := s.process(received)
		if err != nil {
			log.Printf("connection encountered error, closing: %v", err)
			received.conn.close()
		}
	}
}
