/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	zoosync "salesforce.com/zoolater/sync"
)

type Conn interface {
	net.Conn
	Closed() <-chan struct{}
}

type leaderConn struct {
	ln      *LeaderNet
	addr    raft.ServerAddress
	netConn net.Conn
	closed  *zoosync.Broadcast
}

type LeaderNet struct {
	raft        *raft.Raft
	streamLayer raft.StreamLayer
	mutex       sync.Mutex
	conn        *leaderConn
}

type Cached bool

var (
	ErrLocal = errors.New("leader is local")
)

// Returns cached connections to the Raft leader.
func NewLeaderNet(raft *raft.Raft, streamLayer raft.StreamLayer) *LeaderNet {
	return &LeaderNet{
		raft:        raft,
		streamLayer: streamLayer,
	}
}

func (ln *LeaderNet) Dial(timeout time.Duration) (Conn, Cached, error) {
	ln.mutex.Lock()
	defer ln.mutex.Unlock()
	if ln.conn != nil {
		return ln.conn, Cached(true), nil
	}

	addr := ln.raft.Leader()
	if addr == "" {
		return nil, Cached(false), errors.New("No known leader")
	}
	if addr == raft.ServerAddress(ln.streamLayer.Addr().String()) {
		return nil, Cached(false), ErrLocal
	}
	conn, err := ln.streamLayer.Dial(addr, timeout)
	if err != nil {
		return nil, Cached(false), err
	}
	ln.conn = &leaderConn{
		ln:      ln,
		addr:    addr,
		netConn: conn,
		closed:  zoosync.NewBroadcast(),
	}
	return ln.conn, Cached(false), nil
}

func (conn *leaderConn) Close() error {
	conn.ln.mutex.Lock()
	if conn.ln.conn == conn {
		conn.ln.conn = nil
	}
	conn.ln.mutex.Unlock()
	err := conn.netConn.Close()
	conn.closed.Notify()
	return err
}

func (conn *leaderConn) Closed() <-chan struct{} {
	return conn.closed.C
}

func (conn *leaderConn) Read(b []byte) (n int, err error) {
	return conn.netConn.Read(b)
}

func (conn *leaderConn) Write(b []byte) (n int, err error) {
	return conn.netConn.Write(b)
}

func (conn *leaderConn) LocalAddr() net.Addr {
	return conn.netConn.LocalAddr()
}

func (conn *leaderConn) RemoteAddr() net.Addr {
	return conn.netConn.RemoteAddr()
}

func (conn *leaderConn) SetDeadline(t time.Time) error {
	return conn.netConn.SetDeadline(t)
}

func (conn *leaderConn) SetReadDeadline(t time.Time) error {
	return conn.netConn.SetReadDeadline(t)
}

func (conn *leaderConn) SetWriteDeadline(t time.Time) error {
	return conn.netConn.SetWriteDeadline(t)
}
