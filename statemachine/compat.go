/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package statemachine

import "salesforce.com/zoolater/proto"

func (t *Tree) GetChildren(ctx *context, req *proto.GetChildrenRequest) (*proto.GetChildrenResponse, RegisterEvents, proto.ErrCode) {
	req2 := &proto.GetChildren2Request{
		Path:  req.Path,
		Watch: req.Watch,
	}
	resp2, register, err := t.GetChildren2(ctx, req2)
	if err != proto.ErrOk {
		return nil, nil, err
	}
	resp := &proto.GetChildrenResponse{
		Children: resp2.Children,
	}
	return resp, register, proto.ErrOk
}
