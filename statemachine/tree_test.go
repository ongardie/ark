/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package statemachine

import (
	"reflect"
	"testing"

	"salesforce.com/zoolater/proto"
)

var ctx *context = &context{
	zxid: 10,
	time: 2016,
}

func Test_tree_splitPath0(t *testing.T) {
	components := splitPath("/")
	if !reflect.DeepEqual(components, []proto.Component{}) {
		t.Fatalf("Unexpected split: %#v", components)
	}
}

func Test_tree_splitPath1(t *testing.T) {
	components := splitPath("/hello")
	if !reflect.DeepEqual(components, []proto.Component{"hello"}) {
		t.Fatalf("Unexpected split: %#v", components)
	}
}

func Test_tree_splitPath2(t *testing.T) {
	components := splitPath("/hello/world")
	if !reflect.DeepEqual(components, []proto.Component{"hello", "world"}) {
		t.Fatalf("Unexpected split: %#v", components)
	}
}

func Test_tree_joinPath0(t *testing.T) {
	path := joinPath([]proto.Component{})
	if path != "/" {
		t.Fatalf("join returned '%v', expected '/'", path)
	}
}

func Test_tree_joinPath1(t *testing.T) {
	path := joinPath([]proto.Component{"hello"})
	if path != "/hello" {
		t.Fatalf("join returned '%v', expected '/hello'", path)
	}
}

func Test_tree_joinPath2(t *testing.T) {
	path := joinPath([]proto.Component{"hello", "world"})
	if path != "/hello/world" {
		t.Fatalf("join returned '%v', expected '/hello/world'", path)
	}
}

func Test_tree_Create(t *testing.T) {
	t0 := NewTree()
	t1, resp, _, err := t0.Create(ctx, &proto.CreateRequest{
		Path: "/hello",
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	if len(t1.children) != 1 {
		t.Fatalf("Create should have created child")
	}
	if resp.Path != "/hello" {
		t.Fatalf("Got unexpected response path: %v", resp.Path)
	}
	_, _, _, err = t1.Create(ctx, &proto.CreateRequest{
		Path: "/hello/world",
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
}

func Test_tree_Delete(t *testing.T) {
	t0 := NewTree()
	t1, _, _, err := t0.Create(ctx, &proto.CreateRequest{
		Path: "/hello",
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	t2, _, _, err := t1.Delete(ctx, &proto.DeleteRequest{
		Path: "/hello",
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	resp, _, err := t2.GetChildren2(ctx, &proto.GetChildren2Request{
		Path: "/",
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	if len(resp.Children) > 0 {
		t.Fatalf("Unexpected children: %#v", resp.Children)
	}
}

func Test_tree_GetChildren2(t *testing.T) {
	t0 := NewTree()
	t1, _, _, err := t0.Create(ctx, &proto.CreateRequest{
		Path: "/foo",
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	t2, _, _, err := t1.Create(ctx, &proto.CreateRequest{
		Path: "/bar",
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	resp, _, err := t2.GetChildren2(ctx, &proto.GetChildren2Request{
		Path: "/",
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	if !reflect.DeepEqual(resp.Children, []proto.Component{"bar", "foo"}) {
		t.Fatalf("Unexpected children: %#v", resp.Children)
	}
}

func Test_tree_GetData(t *testing.T) {
	t0 := NewTree()
	t1, _, _, err := t0.Create(ctx, &proto.CreateRequest{
		Path: "/hello",
		Data: []byte("world"),
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	resp, _, err := t1.GetData(ctx, &proto.GetDataRequest{
		Path: "/hello",
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	if string(resp.Data) != "world" {
		t.Fatalf("Unexpected data: %#v", resp.Data)
	}
}

func Test_tree_SetData(t *testing.T) {
	t0 := NewTree()
	t1, _, _, err := t0.Create(ctx, &proto.CreateRequest{
		Path: "/hello",
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	t2, _, _, err := t1.SetData(ctx, &proto.SetDataRequest{
		Path:    "/hello",
		Data:    []byte("go"),
		Version: 0,
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	_, _, _, err = t2.SetData(ctx, &proto.SetDataRequest{
		Path:    "/hello",
		Data:    []byte("badver"),
		Version: 0,
	})
	if err != proto.ErrBadVersion {
		t.Fatalf("Expected ErrBadVersion, got: %v", err.Error())
	}

	resp, _, err := t2.GetData(ctx, &proto.GetDataRequest{
		Path: "/hello",
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	if string(resp.Data) != "go" {
		t.Fatalf("Unexpected data: %#v", resp.Data)
	}
}
