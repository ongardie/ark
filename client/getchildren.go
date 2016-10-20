/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package client

import (
	"fmt"

	"salesforce.com/zoolater/jute"
	"salesforce.com/zoolater/proto"
)

type GetChildrenResponse struct {
	Xid      proto.Xid
	Children []proto.Component
	Stat     proto.Stat
}

func (client *Client) GetChildren(
	path proto.Path,
	watcher func(proto.EventType, proto.Path),
	handler func(GetChildrenResponse, error)) {
	req := proto.GetChildren2Request{
		Path:  path,
		Watch: watcher != nil,
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		handler(GetChildrenResponse{}, err)
		return
	}
	client.Request(proto.OpGetChildren2, reqBuf, &Watcher{
		[]proto.EventType{proto.EventNodeDeleted, proto.EventNodeChildrenChanged},
		watcher,
	}, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(GetChildrenResponse{},
				fmt.Errorf("Error in GetChildren(%v): %v", path, reply.Err.Error()))
			return
		}
		var resp proto.GetChildren2Response
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(GetChildrenResponse{}, err)
			return
		}
		handler(GetChildrenResponse{
			Xid:      reply.Xid,
			Children: resp.Children,
			Stat:     resp.Stat,
		}, nil)
	})
}

func (client *Client) GetChildrenSync(
	path proto.Path,
	watcher func(proto.EventType, proto.Path)) (
	GetChildrenResponse,
	error) {
	type pair struct {
		resp GetChildrenResponse
		err  error
	}
	ch := make(chan pair)
	client.GetChildren(path, watcher, func(resp GetChildrenResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}
