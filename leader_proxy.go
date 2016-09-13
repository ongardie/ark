/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type leaderProxy struct {
	raft        *raft.Raft
	streamLayer raft.StreamLayer
	mutex       sync.Mutex
	connAddr    raft.ServerAddress
	conn        net.Conn
}

var (
	local = errors.New("leader is local")
)

func newLeaderProxy(raft *raft.Raft, streamLayer raft.StreamLayer) *leaderProxy {
	p := &leaderProxy{
		raft:        raft,
		streamLayer: streamLayer,
	}
	go p.listen()
	return p
}

func (p *leaderProxy) listen() {
	for {
		conn, err := p.streamLayer.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		log.Printf("accepted inbound leaderProxy connection")
		go p.handle(conn)
	}
}

func (p *leaderProxy) handle(conn net.Conn) {
	for {
		cmd, err := receiveFrame(conn)
		if err != nil {
			log.Printf("Error receiving proxied command (%v), closing connection", err)
			conn.Close()
			return
		}
		log.Printf("Locally applying proxied command")
		future := p.raft.Apply(cmd, 0)
		go func() {
			err := future.Error()
			if err == nil {
				log.Printf("Committed proxied command")
			} else {
				log.Printf("Failed to commit command (%v): closing connection", err)
				conn.Close()
			}
		}()
	}
}

func (p *leaderProxy) getConn() (net.Conn, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.conn != nil {
		return p.conn, nil
	}

	addr := p.raft.Leader()
	if addr == "" {
		return nil, errors.New("No known leader")
	}
	if addr == raft.ServerAddress(p.streamLayer.Addr().String()) {
		return nil, local
	}
	conn, err := p.streamLayer.Dial(addr, time.Second)
	if err != nil {
		return nil, err
	}
	p.connAddr = addr
	p.conn = conn
	log.Printf("Forwarding commands to %v", p.connAddr)
	return conn, nil
}

// Returns as soon as the cmd will be ordered relative to subsequent calls to
// Apply().
func (p *leaderProxy) Apply(cmd []byte) <-chan error {
	errCh := make(chan error, 1)
	conn, err := p.getConn()
	if err == local {
		log.Printf("Applying command locally")
		future := p.raft.Apply(cmd, 0)
		go func() {
			err := future.Error()
			if err != nil {
				errCh <- err
			}
		}()
		return errCh
	}
	if err != nil {
		errCh <- err
		return errCh
	}
	log.Printf("Forwarding command")
	err = sendFrame(conn, cmd)
	if err != nil {
		errCh <- err
		return errCh
	}
	return errCh
}
