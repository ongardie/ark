/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE file or https://opensource.org/licenses/BSD-3-Clause
 */

package main

import (
	"net"

	"github.com/ongardie/ark/intframe"
	"github.com/ongardie/ark/jute"
	"github.com/ongardie/ark/proto"
)

func connect(conn net.Conn, req *proto.ConnectRequest) (*proto.ConnectResponse, error) {
	reqBuf, err := jute.Encode(req)
	if err != nil {
		return nil, err
	}
	err = intframe.Send(conn, reqBuf)
	if err != nil {
		return nil, err
	}
	respBuf, err := intframe.Receive(conn)
	if err != nil {
		return nil, err
	}
	resp := new(proto.ConnectResponse)
	err = jute.Decode(respBuf, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func ping(conn net.Conn) error {
	reqHeader := proto.RequestHeader{
		Xid:    proto.XidPing,
		OpCode: proto.OpPing,
	}
	reqBuf, err := jute.Encode(&reqHeader)
	if err != nil {
		return err
	}
	err = intframe.Send(conn, reqBuf)
	if err != nil {
		return err
	}
	respBuf, err := intframe.Receive(conn)
	if err != nil {
		return err
	}
	var respHeader proto.ResponseHeader
	err = jute.Decode(respBuf, &respHeader)
	if err != nil {
		return err
	}
	return nil
}

func (t *Test) TestZKCP_connect_ok() {
	conn1, err := net.Dial("tcp", "localhost:2181")
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	resp1, err := connect(conn1, &proto.ConnectRequest{
		ProtocolVersion: 0,
		LastZxidSeen:    0,
		Timeout:         0,
		SessionID:       0,
		Passwd:          make([]byte, 16),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp1.SessionID == 0 {
		t.Errorf("Returned session ID is 0, expected positive integer")
	}
	if len(resp1.Passwd) != 16 {
		t.Errorf("Expected 16-byte password, got %v",
			len(resp1.Passwd))
	}
	allZero := true
	for _, b := range resp1.Passwd {
		if b != 0 {
			allZero = false
		}
	}
	if allZero {
		t.Errorf("Expected random password, got %v",
			resp1.Passwd)
	}

	conn2, err := net.Dial("tcp", "localhost:2181")
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	resp2, err := connect(conn2, &proto.ConnectRequest{
		ProtocolVersion: 0,
		LastZxidSeen:    0,
		Timeout:         0,
		SessionID:       resp1.SessionID,
		Passwd:          resp1.Passwd,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp2.SessionID != resp1.SessionID {
		t.Errorf("Returned session ID (%v) doesn't match original %v",
			resp2.SessionID, resp1.SessionID)
	}
	err = ping(conn1)
	if err == nil {
		t.Errorf("Expected ping on first connection to fail, got nil")
	}
	err = ping(conn2)
	if err != nil {
		t.Errorf("Expected ping to succeed on second connection, got: %v", err)
	}
}

func (t *Test) TestZKCP_connect_barelyTolerable() {
	conn, err := net.Dial("tcp", "localhost:2181")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	resp, err := connect(conn, &proto.ConnectRequest{
		ProtocolVersion: 2147483647,
		LastZxidSeen:    0,
		Timeout:         2147483647,
		SessionID:       0,
		Passwd:          []byte{255, 1, 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.SessionID == 0 {
		t.Errorf("Returned session ID is 0, expected positive integer")
	}
	err = ping(conn)
	if err != nil {
		t.Fatal(err)
	}
}

func (t *Test) TestZKCP_connect_barelyTolerable2() {
	conn, err := net.Dial("tcp", "localhost:2181")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	resp, err := connect(conn, &proto.ConnectRequest{
		ProtocolVersion: -2147483648,
		LastZxidSeen:    -9223372036854775808,
		Timeout:         -2147483648,
		SessionID:       0,
		Passwd:          []byte{255, 1, 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.SessionID == 0 {
		t.Errorf("Returned session ID is 0, expected positive integer")
	}
	err = ping(conn)
	if err != nil {
		t.Fatal(err)
	}
}

// This test shows that Apache ZooKeeper may deny connections based on
// lastZxidSeen. It's skipped since ark ignores the field entirely.
func (t *Test) TestZKCP_connect_lastZxidTooHigh() {
	t.SkipNow()
	conn, err := net.Dial("tcp", "localhost:2181")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	resp, err := connect(conn, &proto.ConnectRequest{
		ProtocolVersion: 0,
		LastZxidSeen:    9223372036854775807,
		Timeout:         0,
		SessionID:       0,
		Passwd:          nil,
	})
	if err == nil {
		t.Errorf("Expected connect to return error, got nil")
		if resp.SessionID != 0 {
			t.Errorf("Returned session ID is %v, expected 0",
				resp.SessionID)
		}
		err = ping(conn)
		if err == nil {
			t.Errorf("Expected ping to return error, got nil")
		}
	}
}

func (t *Test) TestZKCP_connect_badSessionID() {
	conn, err := net.Dial("tcp", "localhost:2181")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	resp, err := connect(conn, &proto.ConnectRequest{
		ProtocolVersion: 0,
		LastZxidSeen:    0,
		Timeout:         0,
		SessionID:       283523,
		Passwd:          []byte{255, 1, 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.SessionID != 0 {
		t.Errorf("Returned session ID is %v, expected 0",
			resp.SessionID)
	}
	err = ping(conn)
	if err == nil {
		t.Errorf("Expected ping to return error, got nil")
	}
}

func (t *Test) TestZKCP_connect_badPassword() {
	conn1, err := net.Dial("tcp", "localhost:2181")
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()
	resp1, err := connect(conn1, &proto.ConnectRequest{
		ProtocolVersion: 0,
		LastZxidSeen:    0,
		Timeout:         0,
		SessionID:       0,
		Passwd:          nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp1.SessionID == 0 {
		t.Errorf("Returned session ID is %v, expected non-zero",
			resp1.SessionID)
	}

	conn2, err := net.Dial("tcp", "localhost:2181")
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()
	resp2, err := connect(conn2, &proto.ConnectRequest{
		ProtocolVersion: 0,
		LastZxidSeen:    0,
		Timeout:         0,
		SessionID:       resp1.SessionID,
		Passwd:          []byte{255, 1, 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp2.SessionID != 0 {
		t.Errorf("Returned session ID for bad password is %v, expected 0",
			resp2.SessionID)
	}
	err = ping(conn1)
	if err != nil {
		// Apache ZooKeeper trunk fails this: closes the connection. The Client can
		// reopen, though.
		t.Errorf("Expected ping to succeed on first connection, got: %v", err)
	}
	err = ping(conn2)
	if err == nil {
		t.Errorf("Expected ping to return error with bad password, got nil")
	}

	conn3, err := net.Dial("tcp", "localhost:2181")
	if err != nil {
		t.Fatal(err)
	}
	_, err = connect(conn3, &proto.ConnectRequest{
		ProtocolVersion: 0,
		LastZxidSeen:    0,
		Timeout:         0,
		SessionID:       resp1.SessionID,
		Passwd:          resp1.Passwd,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = ping(conn3)
	if err != nil {
		t.Errorf("Expected ping to succeed with good password, got: %v", err)
	}
}
