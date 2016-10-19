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

type StatResponse struct {
	Xid  proto.Xid
	Stat proto.Stat
}

func (client *Client) Stat(
	path proto.Path,
	watcher func(proto.EventType, proto.Path),
	handler func(StatResponse, error)) {
	req := proto.ExistsRequest{
		Path:  path,
		Watch: watcher != nil,
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		handler(StatResponse{}, err)
	}
	client.Request(proto.OpExists, reqBuf, &Watcher{
		[]proto.EventType{proto.EventNodeCreated, proto.EventNodeDeleted, proto.EventNodeDataChanged},
		watcher,
	}, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(StatResponse{},
				fmt.Errorf("Error in Stat(%v): %v", path, reply.Err.Error()))
		}
		var resp proto.ExistsResponse
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(StatResponse{}, err)
		}
		handler(StatResponse{
			Xid:  reply.Xid,
			Stat: resp.Stat,
		}, nil)
	})
}

func (client *Client) StatSync(
	path proto.Path,
	watcher func(proto.EventType, proto.Path)) (
	StatResponse,
	error) {
	type pair struct {
		resp StatResponse
		err  error
	}
	ch := make(chan pair)
	client.Stat(path, watcher, func(resp StatResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}
