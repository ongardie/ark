/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package client

import (
	"fmt"

	"salesforce.com/ark/jute"
	"salesforce.com/ark/proto"
)

type ExistsResponse struct {
	Stat proto.Stat
}

func (conn *Conn) ExistsAsync(path proto.Path,
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
	conn.RequestAsync(proto.OpExists, reqBuf, func(reply Reply) {
		if watcher != nil {
			if reply.Err == proto.ErrOk {
				conn.Client().RegisterWatcher(watcher, path,
					proto.EventNodeDeleted, proto.EventNodeDataChanged)
			} else if reply.Err == proto.ErrNoNode {
				conn.Client().RegisterWatcher(watcher, path,
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
			Stat: resp.Stat,
		}, nil)
	})
}

func (conn *Conn) Exists(path proto.Path, watcher Watcher) (ExistsResponse, error) {
	type pair struct {
		resp ExistsResponse
		err  error
	}
	ch := make(chan pair)
	conn.ExistsAsync(path, watcher, func(resp ExistsResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}

func (client *Client) Exists(path proto.Path, watcher Watcher) (ExistsResponse, error) {
	return client.Conn().Exists(path, watcher)
}
