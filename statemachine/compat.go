/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package statemachine

import "salesforce.com/ark/proto"

func (t *Tree) GetChildren(ctx *context, req *proto.GetChildrenRequest) (*proto.GetChildrenResponse, RegisterEvents, proto.ErrCode) {
	req2 := &proto.GetChildren2Request{
		Path:  req.Path,
		Watch: req.Watch,
	}
	resp2, register, err := t.GetChildren2(ctx, req2)
	if err != proto.ErrOk {
		return nil, register, err
	}
	resp := &proto.GetChildrenResponse{
		Children: resp2.Children,
	}
	return resp, register, proto.ErrOk
}

func (t *Tree) Create(ctx *context, req *proto.CreateRequest) (*Tree, *proto.CreateResponse, NotifyEvents, proto.ErrCode) {
	req2 := &proto.Create2Request{
		Path: req.Path,
		Data: req.Data,
		ACL:  req.ACL,
		Mode: req.Mode,
	}
	tree, resp2, notify, err := t.Create2(ctx, req2)
	if err != proto.ErrOk {
		return nil, nil, notify, err
	}
	resp := &proto.CreateResponse{
		Path: resp2.Path,
	}
	return tree, resp, notify, err
}
