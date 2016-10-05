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

type Subport byte

type demuxStreamLayer struct {
	real    raft.StreamLayer
	mutex   sync.Mutex
	targets map[Subport]*demuxTarget
}

type demuxTarget struct {
	port   Subport
	stream *demuxStreamLayer
	queue  chan net.Conn
}

// Share a listener on a single TCP port across multiple sub-protocols,
// as identified by Subport integers.
func NewDemuxStreamLayer(real raft.StreamLayer, first Subport, subports ...Subport) map[Subport]raft.StreamLayer {
	stream := &demuxStreamLayer{
		real:    real,
		targets: make(map[Subport]*demuxTarget),
	}
	ret := make(map[Subport]raft.StreamLayer)
	for _, port := range append(subports, first) {
		target := &demuxTarget{
			port:   port,
			stream: stream,
			queue:  make(chan net.Conn, 5),
		}
		stream.targets[port] = target
		ret[port] = target
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
		log.Printf("Error reading sub-port byte: %v", err)
		conn.Close()
		return
	}
	stream.enqueue(Subport(buf[0]), conn)
}

func (stream *demuxStreamLayer) enqueue(port Subport, conn net.Conn) {
	stream.mutex.Lock()
	defer stream.mutex.Unlock()
	target, ok := stream.targets[port]
	if !ok {
		conn.Close()
		log.Printf("Got connection for unknown target")
		return
	}
	select {
	case target.queue <- conn:
		log.Printf("Queued connection for target subport %v", port)
	default:
		conn.Close()
		log.Printf("Got connection for target subport %v with full queue", port)
	}
}

func (target *demuxTarget) Accept() (net.Conn, error) {
	conn := <-target.queue
	log.Printf("Returning connection for target subport %v", target.port)
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
	delete(target.stream.targets, target.port)
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
	_, err = conn.Write([]byte{byte(target.port)})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}
