/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	zk "salesforce.com/ark/client"
	"salesforce.com/ark/jute"
	"salesforce.com/ark/proto"
)

var multiEnd = proto.MultiHeader{
	Type: -1,
	Done: true,
	Err:  -1,
}

// This test shows that you can't send a checkVersion request to Apache
// ZooKeeper outside of a Multi Op. Current versions will crash with a
// NullPointerException and need to be restarted (TODO: file this).
func (t *Test) TestZKCP_Multi_soloCheckVersion() {
	t.SkipNow()
	client := makeClient(t)
	defer client.Close()

	req := proto.CheckVersionRequest{
		Path:    "/",
		Version: 89235,
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		t.Fatal(err)
	}
	client.Conn().RequestSync(proto.OpCheckVersion, reqBuf)
}

func (t *Test) TestZKCP_Multi_noOps() {
	client := makeClient(t)
	defer client.Close()

	reqBuf, err := jute.Encode(&multiEnd)
	if err != nil {
		t.Fatal(err)
	}
	result := client.Conn().RequestSync(proto.OpMulti, reqBuf)
	if result.Err != proto.ErrOk {
		t.Errorf("Multi with no ops returned err: %v", result.Err.Error())
	}
	resp := proto.MultiHeader{}
	more, err := jute.DecodeSome(result.Buf, &resp)
	if err != nil {
		t.Fatal(err)
	}
	if resp != multiEnd {
		t.Errorf("Unexpected multi header: %+v", resp)
	}
	if len(more) > 0 {
		t.Errorf("Unexpected %v extra bytes after multi header", len(more))
	}
}

func (t *Test) TestZKCP_Multi_doneWithBadTypeAndErr() {
	client := makeClient(t)
	defer client.Close()

	req := proto.MultiHeader{
		Type: 93,
		Done: true,
		Err:  3,
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		t.Fatal(err)
	}
	result := client.Conn().RequestSync(proto.OpMulti, reqBuf)
	if result.Err != proto.ErrOk {
		t.Errorf("Multi with done set on bad op returned err: %v", result.Err.Error())
	}
	resp := proto.MultiHeader{}
	more, err := jute.DecodeSome(result.Buf, &resp)
	if err != nil {
		t.Fatal(err)
	}
	if resp != multiEnd {
		t.Errorf("Unexpected multi header: %+v", resp)
	}
	if len(more) > 0 {
		t.Errorf("Unexpected %v extra bytes after multi header", len(more))
	}
}

func (t *Test) TestZKCP_Multi_opWithErrSet() {
	client := makeClient(t)
	defer client.Close()

	_, err := client.Create("/hi", []byte("x"), OpenACL, proto.ModePersistent)
	if err != nil {
		t.Fatal(err)
	}

	header := proto.MultiHeader{
		Type: proto.OpSetData,
		Done: false,
		Err:  3,
	}
	op := proto.SetDataRequest{
		Path: "/hi",
		Data: []byte("hello"),
	}

	headerBuf, err := jute.Encode(&header)
	if err != nil {
		t.Fatal(err)
	}
	opBuf, err := jute.Encode(&op)
	if err != nil {
		t.Fatal(err)
	}
	doneBuf, err := jute.Encode(&multiEnd)
	if err != nil {
		t.Fatal(err)
	}
	buf := append(append(headerBuf, opBuf...), doneBuf...)
	result := client.Conn().RequestSync(proto.OpMulti, buf)
	if result.Err != proto.ErrOk {
		t.Errorf("Multi with err set on op returned err: %v", result.Err.Error())
	}
	resp := proto.MultiHeader{}
	_, err = jute.DecodeSome(result.Buf, &resp)
	if err != nil {
		t.Fatal(err)
	}
	if (resp != proto.MultiHeader{
		Type: proto.OpSetData,
		Done: false,
		Err:  proto.ErrOk,
	}) {
		t.Errorf("Unexpected multi header: %+v", resp)
	}
}

func (t *Test) TestZKCP_Multi_normal() {
	client := makeClient(t)
	defer client.Close()

	resp, err := client.Multi(
		zk.MultiCreate("/x", []byte("h"), OpenACL, proto.ModePersistent),
		zk.MultiCheckVersion("/x", 0),
		zk.MultiSetData("/x", []byte("yo"), 0),
		zk.MultiCheckVersion("/x", 1))
	if err != nil {
		t.Error(err)
	}
	_, err = resp.Create(0)
	if err != nil {
		t.Error(err)
	}
	_, err = resp.CheckVersion(1)
	if err != nil {
		t.Error(err)
	}
	_, err = resp.SetData(2)
	if err != nil {
		t.Error(err)
	}
	_, err = resp.CheckVersion(3)
	if err != nil {
		t.Error(err)
	}
}
