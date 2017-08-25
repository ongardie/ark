/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE file or https://opensource.org/licenses/BSD-3-Clause
 */

package statemachine

import (
	"reflect"
	"testing"

	"github.com/ongardie/ark/proto"
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

func Test_tree_Exists(t *testing.T) {
	t0 := NewTree()
	t1, _, _, err := t0.Create(ctx, &proto.CreateRequest{
		Path: "/hello",
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	resp, register, err := t1.Exists(ctx, &proto.ExistsRequest{
		Path:  "/hello",
		Watch: true,
	})
	if err != proto.ErrOk {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	if resp == nil {
		t.Fatalf("Expected response")
	}
	if len(register) != 2 {
		t.Fatalf("Missing watch registration for node deletion/data")
	}
	resp, register, err = t1.Exists(ctx, &proto.ExistsRequest{
		Path:  "/hello/world/foo",
		Watch: true,
	})
	if err != proto.ErrNoNode {
		t.Fatalf("Unexpected error: %v", err.Error())
	}
	if resp != nil {
		t.Fatalf("Unexpected response: %v", resp)
	}
	if len(register) != 1 {
		t.Fatalf("Missing watch registration for node creation")
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
