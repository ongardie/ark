/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE file or https://opensource.org/licenses/BSD-3-Clause
 */

package client

import (
	"fmt"

	"github.com/ongardie/ark/jute"
	"github.com/ongardie/ark/proto"
)

type GetChildrenResponse struct {
	Children []proto.Component
	Stat     proto.Stat
}

func (conn *Conn) GetChildrenAsync(
	path proto.Path,
	watcher Watcher,
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
	conn.RequestAsync(proto.OpGetChildren2, reqBuf, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(GetChildrenResponse{},
				fmt.Errorf("Error in GetChildren(%v): %v", path, reply.Err.Error()))
			return
		}
		if watcher != nil {
			conn.Client().RegisterWatcher(watcher, path,
				proto.EventNodeDeleted, proto.EventNodeChildrenChanged)
		}
		var resp proto.GetChildren2Response
		err = jute.Decode(reply.Buf, &resp)
		if err != nil {
			handler(GetChildrenResponse{}, err)
			return
		}
		handler(GetChildrenResponse{
			Children: resp.Children,
			Stat:     resp.Stat,
		}, nil)
	})
}

func (conn *Conn) GetChildren(path proto.Path, watcher Watcher) (GetChildrenResponse, error) {
	type pair struct {
		resp GetChildrenResponse
		err  error
	}
	ch := make(chan pair)
	conn.GetChildrenAsync(path, watcher, func(resp GetChildrenResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}

func (client *Client) GetChildren(path proto.Path, watcher Watcher) (GetChildrenResponse, error) {
	return client.Conn().GetChildren(path, watcher)
}
