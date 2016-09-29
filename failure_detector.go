/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"salesforce.com/zoolater/leadernet"
	"salesforce.com/zoolater/statemachine"
)

type failureDetector struct {
	stateMachine      *statemachine.StateMachine
	dialer            *leadernet.LeaderNet
	serverId          uint64
	lastContactMutex  sync.Mutex
	lastContact       map[uint64]time.Time
	lastLeaderContact time.Time
}

func newFailureDetector(stateMachine *statemachine.StateMachine,
	raft *raft.Raft, streamLayer raft.StreamLayer, serverId uint64,
	interval time.Duration) *failureDetector {
	fd := &failureDetector{
		stateMachine: stateMachine,
		dialer:       leadernet.New(raft, streamLayer),
		serverId:     serverId,
		lastContact:  make(map[uint64]time.Time),
	}
	go fd.listen(streamLayer)
	go fd.heartbeatLoop(interval)
	return fd
}

func (fd *failureDetector) listen(streamLayer raft.StreamLayer) {
	for {
		conn, err := streamLayer.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		log.Printf("accepted inbound peer failure detector connection")
		go fd.handle(conn)
	}
}

func sendByte(conn net.Conn) error {
	_, err := conn.Write([]byte{1})
	return err
}

func recvByte(conn net.Conn) error {
	buf := []byte{0}
	_, err := io.ReadFull(conn, buf)
	return err
}

func (fd *failureDetector) handle(conn net.Conn) {
	var serverId uint64
	err := binary.Read(conn, binary.BigEndian, &serverId)
	if err != nil {
		log.Printf("Error receiving server ID (%v), closing connection", err)
		conn.Close()
		return
	}

	for {
		err := recvByte(conn)
		if err != nil {
			log.Printf("Error receiving ping (%v), closing connection", err)
			conn.Close()
			return
		}
		fd.lastContactMutex.Lock()
		fd.lastContact[serverId] = time.Now()
		fd.lastContactMutex.Unlock()
		err = sendByte(conn)
		if err != nil {
			log.Printf("Error sending pong (%v), closing connection", err)
			conn.Close()
			return
		}
	}
}

func (fd *failureDetector) heartbeatLoop(interval time.Duration) {
	for {
		<-time.After(interval)
		go fd.heartbeat()
	}
}

func (fd *failureDetector) heartbeat() {
	err := fd.tryHeartbeat()
	if err != nil {
		log.Printf("heartbeat failed: %v", err)
	}
}

func (fd *failureDetector) tryHeartbeat() error {
	conn, cached, err := fd.dialer.Dial(time.Second)
	start := time.Now()
	if err == leadernet.ErrLocal {
		fd.lastContactMutex.Lock()
		fd.lastContact[fd.serverId] = start
		fd.lastContactMutex.Unlock()
		return nil
	}
	if err != nil {
		return err
	}

	if !cached {
		log.Printf("Sending heartbeats to %v from %v", conn.RemoteAddr(), fd.serverId)
		err = binary.Write(conn, binary.BigEndian, fd.serverId)
		if err != nil {
			return err
		}
	}

	err = sendByte(conn)
	if err != nil {
		conn.Close()
		return err
	}

	err = recvByte(conn)
	if err != nil {
		conn.Close()
		return err
	}

	fd.lastContactMutex.Lock()
	if fd.lastLeaderContact.Before(start) {
		fd.lastLeaderContact = start
	}
	fd.lastContactMutex.Unlock()
	return nil
}

func (fd *failureDetector) LastContact(serverId uint64) time.Time {
	fd.lastContactMutex.Lock()
	defer fd.lastContactMutex.Unlock()
	return fd.lastContact[serverId]
}

func (fd *failureDetector) LastLeaderContact() time.Time {
	fd.lastContactMutex.Lock()
	defer fd.lastContactMutex.Unlock()
	return fd.lastLeaderContact
}
