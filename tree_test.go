/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"reflect"
	"testing"
)

var ctx *Context = &Context{
	zxid: 10,
	time: 2016,
}

func Test_tree_splitPath0(t *testing.T) {
	components := splitPath("/")
	if !reflect.DeepEqual(components, []Component{}) {
		t.Fatalf("Unexpected split: %#v", components)
	}
}

func Test_tree_splitPath1(t *testing.T) {
	components := splitPath("/hello")
	if !reflect.DeepEqual(components, []Component{"hello"}) {
		t.Fatalf("Unexpected split: %#v", components)
	}
}

func Test_tree_splitPath2(t *testing.T) {
	components := splitPath("/hello/world")
	if !reflect.DeepEqual(components, []Component{"hello", "world"}) {
		t.Fatalf("Unexpected split: %#v", components)
	}
}

func Test_tree_joinPath0(t *testing.T) {
	path := joinPath([]Component{})
	if path != "/" {
		t.Fatalf("join returned '%v', expected '/'", path)
	}
}

func Test_tree_joinPath1(t *testing.T) {
	path := joinPath([]Component{"hello"})
	if path != "/hello" {
		t.Fatalf("join returned '%v', expected '/hello'", path)
	}
}

func Test_tree_joinPath2(t *testing.T) {
	path := joinPath([]Component{"hello", "world"})
	if path != "/hello/world" {
		t.Fatalf("join returned '%v', expected '/hello/world'", path)
	}
}

func Test_tree_Create(t *testing.T) {
	watches := WatchUpdates{}
	t0 := NewTree()
	t1, resp, err := t0.Create(ctx, &CreateRequest{
		Path: Path("/hello"),
	}, &watches)
	if err != errOk {
		t.Fatalf("Unexpected error: %v", err.toError())
	}
	if len(t1.children) != 1 {
		t.Fatalf("Create should have created child")
	}
	if resp.Path != "/hello" {
		t.Fatalf("Got unexpected response path: %v", resp.Path)
	}
	_, _, err = t1.Create(ctx, &CreateRequest{
		Path: Path("/hello/world"),
	}, &watches)
	if err != errOk {
		t.Fatalf("Unexpected error: %v", err.toError())
	}
}

func Test_tree_GetChildren(t *testing.T) {
	watches := WatchUpdates{}
	t0 := NewTree()
	t1, _, err := t0.Create(ctx, &CreateRequest{
		Path: Path("/foo"),
	}, &watches)
	if err != errOk {
		t.Fatalf("Unexpected error: %v", err.toError())
	}
	t2, _, err := t1.Create(ctx, &CreateRequest{
		Path: Path("/bar"),
	}, &watches)
	if err != errOk {
		t.Fatalf("Unexpected error: %v", err.toError())
	}
	resp, err := t2.GetChildren(ctx, &getChildren2Request{
		Path: "/",
	}, &watches)
	if err != errOk {
		t.Fatalf("Unexpected error: %v", err.toError())
	}
	if !reflect.DeepEqual(resp.Children, []Component{"bar", "foo"}) {
		t.Fatalf("Unexpected children: %#v", resp.Children)
	}
}

func Test_tree_GetData(t *testing.T) {
	watches := WatchUpdates{}
	t0 := NewTree()
	t1, _, err := t0.Create(ctx, &CreateRequest{
		Path: Path("/hello"),
		Data: []byte("world"),
	}, &watches)
	if err != errOk {
		t.Fatalf("Unexpected error: %v", err.toError())
	}
	resp, err := t1.GetData(ctx, &getDataRequest{
		Path: "/hello",
	}, &watches)
	if err != errOk {
		t.Fatalf("Unexpected error: %v", err.toError())
	}
	if string(resp.Data) != "world" {
		t.Fatalf("Unexpected data: %#v", resp.Data)
	}
}

func Test_tree_SetData(t *testing.T) {
	watches := WatchUpdates{}
	t0 := NewTree()
	t1, _, err := t0.Create(ctx, &CreateRequest{
		Path: Path("/hello"),
	}, &watches)
	if err != errOk {
		t.Fatalf("Unexpected error: %v", err.toError())
	}
	t2, _, err := t1.SetData(ctx, &SetDataRequest{
		Path:    "/hello",
		Data:    []byte("go"),
		Version: 0,
	}, &watches)
	if err != errOk {
		t.Fatalf("Unexpected error: %v", err.toError())
	}
	_, _, err = t2.SetData(ctx, &SetDataRequest{
		Path:    "/hello",
		Data:    []byte("badver"),
		Version: 0,
	}, &watches)
	if err != errBadVersion {
		t.Fatalf("Expected ErrBadVersion, got: %v", err.toError())
	}

	resp, err := t2.GetData(ctx, &getDataRequest{
		Path: "/hello",
	}, &watches)
	if err != errOk {
		t.Fatalf("Unexpected error: %v", err.toError())
	}
	if string(resp.Data) != "go" {
		t.Fatalf("Unexpected data: %#v", resp.Data)
	}
}
