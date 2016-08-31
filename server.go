/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type StateMachine struct {
	mutex sync.Mutex
	zxid  int64
	tree  *Tree
}

var stateMachine StateMachine = StateMachine{
	tree: NewTree(),
}

func processConnect(ctx *Context, req *connectRequest) *connectResponse {
	stateMachine.mutex.Lock()
	stateMachine.zxid++
	ctx.zxid = stateMachine.zxid
	defer stateMachine.mutex.Unlock()

	resp := &connectResponse{ // TODO: set these like ZooKeeper does
		ProtocolVersion: 12,
		TimeOut:         8790,
		SessionID:       325,
		Passwd:          []byte("wtf"),
	}
	return resp
}

func processRequest(ctx *Context, req interface{}) (interface{}, ErrCode) {
	stateMachine.mutex.Lock()
	stateMachine.zxid++
	ctx.zxid = stateMachine.zxid
	defer stateMachine.mutex.Unlock()

	switch req := req.(type) {

	case *CreateRequest:
		tree, resp, errCode := stateMachine.tree.Create(ctx, req)
		if errCode != errOk {
			return nil, errCode
		}
		stateMachine.tree = tree
		return resp, errOk

	case *getChildren2Request:
		tree, resp, errCode := stateMachine.tree.GetChildren(ctx, req)
		if errCode != errOk {
			return nil, errCode
		}
		stateMachine.tree = tree
		return resp, errOk

	case *getDataRequest:
		tree, resp, errCode := stateMachine.tree.GetData(ctx, req)
		if errCode != errOk {
			return nil, errCode
		}
		stateMachine.tree = tree
		return resp, errOk

	case *SetDataRequest:
		tree, resp, errCode := stateMachine.tree.SetData(ctx, req)
		if errCode != errOk {
			return nil, errCode
		}
		stateMachine.tree = tree
		return resp, errOk

	default:
		return nil, errUnimplemented
	}
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

func handleRequest(conn net.Conn) error {
	log.Printf("Waiting for incoming request")
	header := &requestHeader{}
	more, err := readMessage(conn, header)
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
		err = sendMessage(conn, &pingResponse{}, nil)
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
		Xid: header.Xid,
		Err: errOk,
	}
	log.Printf("Processing request %#v", req)
	resp, errCode := processRequest(&Context{
		time: time.Now().Unix(),
	}, req)
	if len(more) > 0 {
		log.Printf("unexpected bytes after reading %v request: %v", name, more)
	}
	if errCode != errOk {
		respHeader.Err = errCode
		log.Printf("Replying with error header to %v: %+v", name, respHeader)
		err = sendMessage(conn, respHeader, nil)
		if err != nil {
			return fmt.Errorf("Error sending response header to %v: %v", name, err)
		}
		return nil
	}
	if resp == nil {
		return fmt.Errorf("response not set for %v", name)
	}
	log.Printf("reply: %+v value %+v", respHeader, resp)
	err = sendMessage(conn, respHeader, resp)
	if err != nil {
		return fmt.Errorf("Error sending response to %v: %v", name, err)
	}

	return nil
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Print("incoming connection")

	req := &connectRequest{}
	buf, err := readMessage(conn, req)
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
	resp := processConnect(&Context{
		time: time.Now().Unix(),
	}, req)
	var supplement interface{}
	if sendReadOnlyByte {
		supplement = &struct {
			readOnly bool
		}{
			readOnly: false,
		}
	}
	err = sendMessage(conn, resp, supplement)
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
			go handleConnection(conn)
		} else {
			log.Printf("Error from Accept: %v", err)
		}
	}
}
