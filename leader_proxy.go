/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"salesforce.com/zoolater/intframe"

	"github.com/hashicorp/raft"
)

type proxyConn struct {
	addr    raft.ServerAddress
	netConn net.Conn
	closeCh chan struct{}
}

type leaderProxy struct {
	raft        *raft.Raft
	streamLayer raft.StreamLayer
	mutex       sync.Mutex
	conn        *proxyConn
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
		cmd, err := intframe.Receive(conn)
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

func (p *leaderProxy) rejectConn(conn *proxyConn) {
	p.mutex.Lock()
	if p.conn == conn {
		p.conn = nil
	}
	p.mutex.Unlock()
	conn.netConn.Close()
	select {
	case conn.closeCh <- struct{}{}:
	default:
	}
}

func (p *leaderProxy) getConn() (*proxyConn, error) {
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
	p.conn = &proxyConn{
		addr:    addr,
		netConn: conn,
		closeCh: make(chan struct{}, 1),
	}
	go p.waitForReadError(p.conn)
	log.Printf("Forwarding commands to %v", p.conn.addr)
	return p.conn, nil
}

func (p *leaderProxy) waitForReadError(conn *proxyConn) {
	buf := make([]byte, 1)
	_, err := io.ReadFull(conn.netConn, buf)
	if err != nil {
		log.Printf("Proxy connection read error: %v", err)
	} else {
		log.Printf("Leader sent unexpected byte over proxy connection")
	}
	p.rejectConn(conn)
}

// Returns as soon as the cmd will be ordered relative to subsequent calls to
// Apply().
func (p *leaderProxy) Apply(cmd []byte) (errCh <-chan error, doneCh chan<- struct{}) {
	_errCh := make(chan error, 1)
	_doneCh := make(chan struct{}, 1)
	errCh = _errCh
	doneCh = _doneCh

	conn, err := p.getConn()
	if err == local {
		log.Printf("Applying command locally")
		future := p.raft.Apply(cmd, 0)
		go func() {
			err := future.Error()
			if err != nil {
				_errCh <- err
			}
		}()
		return
	}
	if err != nil {
		_errCh <- err
		return
	}
	log.Printf("Forwarding command")
	err = intframe.Send(conn.netConn, cmd)
	if err != nil {
		_errCh <- err
		p.rejectConn(conn)
		return
	}
	go func() {
		select {
		case <-conn.closeCh:
			conn.closeCh <- struct{}{}
			_errCh <- errors.New("connection to leader closed")
		case <-_doneCh:
		}
	}()
	return
}
