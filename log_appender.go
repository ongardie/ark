/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE file or https://opensource.org/licenses/BSD-3-Clause
 */

package main

import (
	"errors"
	"io"
	"log"
	"net"
	"time"

	"github.com/hashicorp/raft"

	"github.com/ongardie/ark/intframe"
)

type logAppender struct {
	raft   *raft.Raft
	dialer *LeaderNet
}

// If leader, appends commands to the local Raft log. Otherwise, forwards
// commands to the Raft leader.
func newLogAppender(raft *raft.Raft, streamLayer raft.StreamLayer) *logAppender {
	a := &logAppender{
		raft:   raft,
		dialer: NewLeaderNet(raft, streamLayer),
	}
	go a.listen(streamLayer)
	return a
}

func (a *logAppender) listen(streamLayer raft.StreamLayer) {
	for {
		conn, err := streamLayer.Accept()
		if err != nil {
			log.Printf("accept error: %v", err)
			continue
		}
		log.Printf("accepted inbound appender connection")
		go a.handle(conn)
	}
}

func (a *logAppender) handle(conn net.Conn) {
	for {
		cmd, err := intframe.Receive(conn)
		if err != nil {
			log.Printf("Error receiving proxied command (%v), closing connection", err)
			conn.Close()
			return
		}
		log.Printf("Locally applying proxied command")
		future := a.raft.Apply(cmd, 0)
		go func() {
			err := future.Error()
			if err == nil {
				log.Printf("Committed proxied command at index %v", future.Index())
			} else {
				log.Printf("Failed to commit command (%v): closing connection", err)
				conn.Close()
			}
		}()
	}
}

func (a *logAppender) waitForReadError(conn net.Conn) {
	buf := make([]byte, 1)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		log.Printf("Proxy connection read error: %v", err)
	} else {
		log.Printf("Leader sent unexpected byte over proxy connection")
	}
	conn.Close()
}

// Returns as soon as the cmd will be ordered relative to subsequent calls to
// Append().
func (a *logAppender) Append(cmd []byte, doneCh <-chan struct{}) <-chan error {
	errCh := make(chan error, 1)

	conn, cached, err := a.dialer.Dial(time.Second)
	if err == ErrLocal {
		log.Printf("Appending command locally")
		future := a.raft.Apply(cmd, 0)
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

	if !cached {
		log.Printf("Forwarding commands to %v", conn.RemoteAddr())
		go a.waitForReadError(conn)
	}

	log.Printf("Forwarding command")
	err = intframe.Send(conn, cmd)
	if err != nil {
		errCh <- err
		conn.Close()
		return errCh
	}
	go func() {
		select {
		case <-conn.Closed():
			errCh <- errors.New("connection to leader closed")
		case <-doneCh:
		}
	}()
	return errCh
}
