/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	cryptoRand "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type Consensus struct {
	mutex        sync.Mutex
	zxid         ZXID
	stateMachine *StateMachine
}

var consensus = Consensus{
	stateMachine: NewStateMachine(),
}

func getContext(conn *Connection) *Context {
	consensus.zxid++
	ctx := &Context{
		zxid:      consensus.zxid,
		time:      time.Now().Unix(),
		sessionId: conn.sessionId,
	}
	ctx.rand = make([]byte, SessionPasswordLen)
	_, err := cryptoRand.Read(ctx.rand)
	if err != nil {
		log.Fatalf("Could not get random bytes: %v", err)
	}
	return ctx
}

func processConnect(conn *Connection, req *connectRequest) (*connectResponse, ErrCode) {
	consensus.mutex.Lock()
	defer consensus.mutex.Unlock()
	resp := &connectResponse{
		ProtocolVersion: 12, // TODO: set this like ZooKeeper does
		TimeOut:         req.TimeOut,
	}
	if req.SessionID == 0 {
		resp.SessionID, resp.Passwd = consensus.stateMachine.createSession(getContext(conn))
	} else {
		resp.SessionID = req.SessionID
		resp.Passwd = req.Passwd
	}
	err := consensus.stateMachine.setConn(resp.SessionID, resp.Passwd, conn)
	if err != errOk {
		return nil, err
	}
	conn.sessionId = resp.SessionID
	return resp, errOk
}

func readMessage(conn net.Conn, msg interface{}) ([]byte, error) {
	buf := make([]byte, 4)
	n, err := io.ReadFull(conn, buf)
	if err != nil {
		return nil, err
	}
	bytes := binary.BigEndian.Uint32(buf[:n])
	log.Printf("Expecting %v bytes", bytes)
	buf = make([]byte, bytes)
	n, err = io.ReadFull(conn, buf)
	if err != nil {
		return nil, err
	}
	return readMore(buf[:n], msg)
}

func readMore(buf []byte, msg interface{}) ([]byte, error) {
	n, err := decodePacket(buf, msg)
	if err != nil {
		return nil, err
	}
	buf = buf[n:]
	log.Printf("DecodePacket returned %v, so %v bytes left", n, len(buf))
	return buf, nil
}

func sendMessage(conn net.Conn, header interface{}, msg interface{}) error {
	var empty struct{}
	log.Printf("sending %#v %#v", header, msg)
	bufSize := 1024
	for {
		buf := make([]byte, bufSize)
		lengthBytes := 4
		if header == nil {
			header = &empty
		}
		headerBytes, err := encodePacket(buf[lengthBytes:], header)
		if err != nil {
			if err == ErrShortBuffer {
				log.Printf("buffer size of %v too small, doubling", bufSize)
				bufSize *= 2
				continue
			}
			return err
		}
		if msg == nil {
			msg = &empty
		}
		msgBytes, err := encodePacket(buf[lengthBytes+headerBytes:], msg)
		if err != nil {
			if err == ErrShortBuffer {
				log.Printf("buffer size of %v too small, doubling", bufSize)
				bufSize *= 2
				continue
			}
			return err
		}
		buf = buf[:lengthBytes+headerBytes+msgBytes]
		binary.BigEndian.PutUint32(buf[:lengthBytes], uint32(headerBytes+msgBytes))
		_, err = conn.Write(buf)
		return err
	}
}

func handleRequest(conn *Connection) error {
	log.Printf("Waiting for incoming request")
	header := &requestHeader{}
	more, err := readMessage(conn.netConn, header)
	if err != nil {
		return fmt.Errorf("error reading request header: %v", err)
	}
	name, ok := opNames[header.Opcode]
	if !ok {
		return fmt.Errorf("unknown opcode: %#v", header)
	}
	log.Printf("Receiving %v", name)
	if header.Opcode == opPing {
		log.Printf("Sending pong")
		err = sendMessage(conn.netConn, &pingResponse{}, nil)
		if err != nil {
			return fmt.Errorf("error sending pong: %v", err)
		}
		return nil
	}
	req := requestStructForOp(header.Opcode)
	if req == nil {
		return fmt.Errorf("no request struct for %v", name)
	}
	more, err = readMore(more, req)
	if err != nil {
		return fmt.Errorf("error reading %v request: %v", name, err)
	}
	log.Printf("Read: %#v", req)

	respHeader := &responseHeader{
		Xid:  header.Xid,
		Zxid: 1, // TODO
		Err:  errOk,
	}
	log.Printf("Processing request %#v", req)

	consensus.mutex.Lock()
	resp, errCode := consensus.stateMachine.processRequest(getContext(conn), req)
	consensus.mutex.Unlock()
	if len(more) > 0 {
		log.Printf("unexpected bytes after reading %v request: %v", name, more)
	}
	if errCode != errOk {
		respHeader.Err = errCode
		log.Printf("Replying with error header to %v: %+v", name, respHeader)
		err = sendMessage(conn.netConn, respHeader, nil)
		if err != nil {
			return fmt.Errorf("Error sending response header to %v: %v", name, err)
		}
		return nil
	}
	if resp == nil {
		return fmt.Errorf("response not set for %v", name)
	}
	log.Printf("reply: %+v value %+v", respHeader, resp)
	err = sendMessage(conn.netConn, respHeader, resp)
	if err != nil {
		return fmt.Errorf("Error sending response to %v: %v", name, err)
	}

	return nil
}

func handleConnection(conn *Connection) {
	defer conn.netConn.Close()
	log.Print("incoming connection")

	req := &connectRequest{}
	buf, err := readMessage(conn.netConn, req)
	if err != nil {
		log.Printf("error reading ConnectRequest: %v", err)
		return
	}
	sendReadOnlyByte := false
	if len(buf) == 1 {
		// Modern ZK clients send 1 more byte to indicate whether they support
		// read-only mode. We ignore it but send a 0 byte back.
		sendReadOnlyByte = true
	} else if len(buf) > 1 {
		log.Printf("unexpected bytes after ConnectRequest: %#v", buf)
		return
	}
	log.Printf("connection request: %#v", req)
	resp, errCode := processConnect(conn, req)
	if errCode != errOk {
		// TODO: what am I supposed to do with this?
		log.Printf("Can't satisfy connection request (%v), dropping", errCode.toError())
		return
	}
	var supplement interface{}
	if sendReadOnlyByte {
		supplement = &struct {
			readOnly bool
		}{
			readOnly: false,
		}
	}
	err = sendMessage(conn.netConn, resp, supplement)
	if err != nil {
		log.Printf("error sending ConnectResponse: %v", err)
		return
	}

	for {
		err = handleRequest(conn)
		if err != nil {
			log.Printf("Error handling request: %v", err)
			break
		}
	}
}

func main() {
	log.Print("listening for ZooKeeper clients on port 2181")
	listener, err := net.Listen("tcp", ":2181")
	if err != nil {
		log.Printf("error listening: %v", err)
		os.Exit(1)
	}
	for {
		conn, err := listener.Accept()
		if err == nil {
			go handleConnection(&Connection{netConn: conn})
		} else {
			log.Printf("Error from Accept: %v", err)
		}
	}
}
