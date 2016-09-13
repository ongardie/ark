/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type demuxStreamLayer struct {
	real    raft.StreamLayer
	mutex   sync.Mutex
	targets map[byte]*demuxTarget
}

type demuxTarget struct {
	discriminator byte
	stream        *demuxStreamLayer
	queue         []net.Conn
}

func NewDemuxStreamLayer(real raft.StreamLayer, first byte, discriminators ...byte) map[byte]raft.StreamLayer {
	stream := &demuxStreamLayer{
		real:    real,
		targets: make(map[byte]*demuxTarget),
	}
	ret := make(map[byte]raft.StreamLayer)
	for _, dis := range append(discriminators, first) {
		target := &demuxTarget{
			discriminator: dis,
			stream:        stream,
		}
		stream.targets[dis] = target
		ret[dis] = target
	}
	return ret
}

func (target *demuxTarget) Accept() (net.Conn, error) {
	target.stream.mutex.Lock()
	defer target.stream.mutex.Unlock()
	for len(target.queue) == 0 {
		conn, err := target.stream.real.Accept()
		if err != nil {
			return nil, err
		}
		buf := []byte{0}
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			return nil, err
		}
		if n == 0 {
			conn.Close()
			return nil, errors.New("Could not read first byte from connection")
		}
		discriminator := buf[0]
		other, ok := target.stream.targets[discriminator]
		if !ok {
			conn.Close()
			return nil, errors.New("Got connection for unknown target")
		}
		if len(other.queue) >= 5 {
			conn.Close()
			return nil, fmt.Errorf("Got connection for target %v with full queue", discriminator)
		}
		other.queue = append(other.queue, conn)
	}
	conn := target.queue[0]
	target.queue = target.queue[1:]
	return conn, nil
}

func (target *demuxTarget) Close() error {
	target.stream.mutex.Lock()
	delete(target.stream.targets, target.discriminator)
	close := (len(target.stream.targets) == 0)
	target.stream.mutex.Unlock()
	if close {
		return target.stream.real.Close()
	} else {
		return nil
	}
}

func (target *demuxTarget) Addr() net.Addr {
	return target.stream.real.Addr()
}

func (target *demuxTarget) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	conn, err := target.stream.real.Dial(address, timeout)
	if err != nil {
		return nil, err
	}
	_, err = conn.Write([]byte{target.discriminator})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}
