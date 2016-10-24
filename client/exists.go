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

type ExistsResponse struct {
	Xid  proto.Xid
	Stat proto.Stat
}

func (client *Client) Exists(
	path proto.Path,
	watcher Watcher,
	handler func(ExistsResponse, error)) {
	req := proto.ExistsRequest{
		Path:  path,
		Watch: watcher != nil,
	}
	reqBuf, err := jute.Encode(&req)
	if err != nil {
		handler(ExistsResponse{}, err)
		return
	}
	client.Request(proto.OpExists, reqBuf, func(reply Reply) {
		if watcher != nil {
			if reply.Err == proto.ErrOk {
				client.RegisterWatcher(watcher, path,
					proto.EventNodeDeleted, proto.EventNodeDataChanged)
			} else if reply.Err == proto.ErrNoNode {
				client.RegisterWatcher(watcher, path,
					proto.EventNodeCreated, proto.EventNodeDataChanged)
			}
		}
		if reply.Err != proto.ErrOk {
			handler(ExistsResponse{},
				fmt.Errorf("Error in Exists(%v): %v", path, reply.Err.Error()))
			return
		}
		var resp proto.ExistsResponse
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(ExistsResponse{}, err)
			return
		}
		handler(ExistsResponse{
			Xid:  reply.Xid,
			Stat: resp.Stat,
		}, nil)
	})
}

func (client *Client) ExistsSync(
	path proto.Path,
	watcher Watcher) (
	ExistsResponse,
	error) {
	type pair struct {
		resp ExistsResponse
		err  error
	}
	ch := make(chan pair)
	client.Exists(path, watcher, func(resp ExistsResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}
