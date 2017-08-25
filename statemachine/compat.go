/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE file or https://opensource.org/licenses/BSD-3-Clause
 */

package statemachine

import "github.com/ongardie/ark/proto"

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
