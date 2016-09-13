/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"io"
	"log"
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
	queue         chan net.Conn
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
			queue:         make(chan net.Conn, 5),
		}
		stream.targets[dis] = target
		ret[dis] = target
	}
	go stream.acceptLoop()
	return ret
}

func (stream *demuxStreamLayer) acceptLoop() {
	for {
		conn, err := stream.real.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}
		go stream.handle(conn)
	}
}

func (stream *demuxStreamLayer) handle(conn net.Conn) {
	buf := []byte{0}
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		log.Printf("Error reading discriminator byte: %v", err)
		conn.Close()
		return
	}
	stream.enqueue(buf[0], conn)
}

func (stream *demuxStreamLayer) enqueue(discriminator byte, conn net.Conn) {
	stream.mutex.Lock()
	defer stream.mutex.Unlock()
	target, ok := stream.targets[discriminator]
	if !ok {
		conn.Close()
		log.Printf("Got connection for unknown target")
		return
	}
	select {
	case target.queue <- conn:
		log.Printf("Queued connection for target %v", discriminator)
	default:
		conn.Close()
		log.Printf("Got connection for target %v with full queue", discriminator)
	}
}

func (target *demuxTarget) Accept() (net.Conn, error) {
	conn := <-target.queue
	log.Printf("Returning connection for target %v", target.discriminator)
	return conn, nil
}

func (target *demuxTarget) Close() error {
	target.stream.mutex.Lock()
	draining := true
	for draining {
		select {
		case conn := <-target.queue:
			conn.Close()
		default:
			draining = false
		}
	}
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
