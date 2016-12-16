/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"net"

	"salesforce.com/ark/intframe"
	"salesforce.com/ark/jute"
	"salesforce.com/ark/proto"
)

// This test shows that if you send a ping with a positive Xid to Apache
// ZooKeeper, it may reply to the ping with an Xid of -2. So clients MUST NOT do
// that.
func (t *Test) TestZKCP_connect_pingWithNormalXid() {
	t.SkipNow()
	conn, err := net.Dial("tcp", "localhost:2181")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = connect(conn, &proto.ConnectRequest{})
	if err != nil {
		t.Fatal(err)
	}
	reqHeader := proto.RequestHeader{
		Xid:    1,
		OpCode: proto.OpPing,
	}
	reqBuf, err := jute.Encode(&reqHeader)
	if err != nil {
		t.Fatal(err)
	}
	err = intframe.Send(conn, reqBuf)
	if err != nil {
		t.Fatal(err)
	}
	respBuf, err := intframe.Receive(conn)
	if err != nil {
		t.Fatal(err)
	}
	var respHeader proto.ResponseHeader
	err = jute.Decode(respBuf, &respHeader)
	if err != nil {
		t.Fatal(err)
	}
	if respHeader.Xid != 1 {
		t.Errorf("Expected Xid of 1 back from pong, got %v",
			respHeader.Xid)
	}
}

// This test shows that ZooKeeper permits anything as the staring Xid on a
// connection.
func (t *Test) TestZKCP_connect_startingXid() {
	conn, err := net.Dial("tcp", "localhost:2181")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = connect(conn, &proto.ConnectRequest{})
	if err != nil {
		t.Fatal(err)
	}
	reqHeader := proto.RequestHeader{
		Xid:    -2,
		OpCode: proto.OpExists,
	}
	reqHeaderBuf, err := jute.Encode(&reqHeader)
	if err != nil {
		t.Fatal(err)
	}
	req := proto.ExistsRequest{
		Path: "/",
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		t.Fatal(err)
	}
	err = intframe.Send(conn, append(reqHeaderBuf, reqBuf...))
	if err != nil {
		t.Fatal(err)
	}
	respBuf, err := intframe.Receive(conn)
	if err != nil {
		t.Fatal(err)
	}
	var respHeader proto.ResponseHeader
	respBuf, err = jute.DecodeSome(respBuf, &respHeader)
	if err != nil {
		t.Fatal(err)
	}
	var resp proto.ExistsResponse
	err = jute.Decode(respBuf, &resp)
	if err != nil {
		t.Fatal(err)
	}
	if respHeader.Err != proto.ErrOk {
		t.Error("header error: %v", respHeader.Err.Error())
	}
}

func (t *Test) TestZKCP_connect_xidOutOfOrder() {
	testZKCP_connext_xids(t, 29, 30)
}

func (t *Test) TestZKCP_connect_xidRepeated() {
	testZKCP_connext_xids(t, 30, 30)
}

func testZKCP_connext_xids(t *Test, xid1 proto.Xid, xid2 proto.Xid) {
	conn, err := net.Dial("tcp", "localhost:2181")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = connect(conn, &proto.ConnectRequest{})
	if err != nil {
		t.Fatal(err)
	}
	reqHeader1 := proto.RequestHeader{
		Xid:    xid1,
		OpCode: proto.OpExists,
	}
	reqHeader1Buf, err := jute.Encode(&reqHeader1)
	if err != nil {
		t.Fatal(err)
	}
	reqHeader2 := proto.RequestHeader{
		Xid:    xid2,
		OpCode: proto.OpExists,
	}
	reqHeader2Buf, err := jute.Encode(&reqHeader2)
	if err != nil {
		t.Fatal(err)
	}
	req := proto.ExistsRequest{
		Path: "/",
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		t.Fatal(err)
	}
	err = intframe.Send(conn, append(reqHeader1Buf, reqBuf...))
	if err != nil {
		t.Fatal(err)
	}
	err = intframe.Send(conn, append(reqHeader2Buf, reqBuf...))
	if err != nil {
		t.Fatal(err)
	}
	respBuf1, err := intframe.Receive(conn)
	if err != nil {
		t.Fatal(err)
	}
	var respHeader1 proto.ResponseHeader
	respBuf1, err = jute.DecodeSome(respBuf1, &respHeader1)
	if err != nil {
		t.Fatal(err)
	}
	var resp1 proto.ExistsResponse
	err = jute.Decode(respBuf1, &resp1)
	if err != nil {
		t.Fatal(err)
	}
	respBuf2, err := intframe.Receive(conn)
	if err != nil {
		t.Fatal(err)
	}
	var respHeader2 proto.ResponseHeader
	respBuf2, err = jute.DecodeSome(respBuf2, &respHeader2)
	if err != nil {
		t.Fatal(err)
	}
	var resp2 proto.ExistsResponse
	err = jute.Decode(respBuf2, &resp2)
	if err != nil {
		t.Fatal(err)
	}
	if respHeader1.Err != proto.ErrOk {
		t.Error("header 1 error: %v", respHeader1.Err.Error())
	}
	if respHeader2.Err != proto.ErrOk {
		t.Error("header 2 error: %v", respHeader2.Err.Error())
	}
}
