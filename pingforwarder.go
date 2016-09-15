/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"encoding/binary"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"salesforce.com/zoolater/leadernet"
	"salesforce.com/zoolater/proto"
	"salesforce.com/zoolater/statemachine"
)

type pingForwarder struct {
	stateMachine *statemachine.StateMachine
	dialer       *leadernet.LeaderNet
	pendingMutex sync.Mutex
	pending      map[proto.SessionId]chan<- error
}

func newPingForwarder(stateMachine *statemachine.StateMachine,
	raft *raft.Raft, streamLayer raft.StreamLayer) *pingForwarder {
	a := &pingForwarder{
		stateMachine: stateMachine,
		dialer:       leadernet.New(raft, streamLayer),
		pending:      make(map[proto.SessionId]chan<- error),
	}
	go a.listen(streamLayer)
	return a
}

func (a *pingForwarder) listen(streamLayer raft.StreamLayer) {
	for {
		conn, err := streamLayer.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		log.Printf("accepted inbound peer ping connection")
		go a.handle(conn)
	}
}

func sendSessionId(conn net.Conn, sessionId proto.SessionId) error {
	return binary.Write(conn, binary.BigEndian, sessionId)
}

func recvSessionId(conn net.Conn) (proto.SessionId, error) {
	var sessionId proto.SessionId
	err := binary.Read(conn, binary.BigEndian, &sessionId)
	return sessionId, err
}

func (a *pingForwarder) handle(conn net.Conn) {
	for {
		sessionId, err := recvSessionId(conn)
		if err != nil {
			log.Printf("Error receiving ping (%v), closing connection", err)
			conn.Close()
			return
		}
		log.Printf("Got ping for %v", sessionId)
		// TODO
		err = sendSessionId(conn, sessionId)
		if err != nil {
			log.Printf("Error sending pong (%v), closing connection", err)
			conn.Close()
			return
		}
	}
}

func (a *pingForwarder) recvPongs(conn net.Conn) {
	for {
		sessionId, err := recvSessionId(conn)
		if err != nil {
			log.Printf("Error receiving ping (%v), closing connection", err)
			conn.Close()
			return
		}
		a.pendingMutex.Lock()
		ch, ok := a.pending[sessionId]
		if ok {
			select {
			case ch <- nil:
			default:
			}
			delete(a.pending, sessionId)
		}
		a.pendingMutex.Unlock()
	}
}

func (a *pingForwarder) Ping(sessionId proto.SessionId) error {
	conn, cached, err := a.dialer.Dial(time.Second)
	if err == leadernet.ErrLocal {
		log.Printf("Handling ping for %v locally", sessionId)
		// TODO
		return nil
	}
	if err != nil {
		return err
	}

	if !cached {
		log.Printf("Forwarding pings to %v", conn.RemoteAddr())
		go a.recvPongs(conn)
	}

	log.Printf("Forwarding ping on session %v", sessionId)
	a.pendingMutex.Lock()
	prev, ok := a.pending[sessionId]
	if ok {
		prev <- errors.New("Replaced by newer ping")
	}
	ch := make(chan error, 1)
	a.pending[sessionId] = ch
	a.pendingMutex.Unlock()

	err = sendSessionId(conn, sessionId)
	if err != nil {
		conn.Close()
		return err
	}
	return <-ch
}
