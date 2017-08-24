/*
 * Copyright (c) 2016, Salesforce.com, Inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license.
 * For full license text, see LICENSE file or https://opensource.org/licenses/BSD-3-Clause
 */

package client

import (
	"errors"
	"fmt"

	"salesforce.com/ark/jute"
	"salesforce.com/ark/proto"
)

type MultiResponse struct {
	Xid     proto.Xid
	Zxid    proto.ZXID
	replies []interface{}
}

type multiOp struct {
	opCode proto.OpCode
	buf    []byte
	err    error
}

type CheckVersionResponse struct {
	Zxid proto.ZXID
}

func (conn *Conn) MultiAsync(ops []multiOp, handler func(MultiResponse, error)) {
	var reqBuf []byte
	for _, op := range ops {
		if op.err != nil {
			handler(MultiResponse{}, op.err)
			return
		}
		reqBuf = append(reqBuf, op.buf...)
	}
	endBuf, err := jute.Encode(&proto.MultiHeader{
		Type: -1,
		Done: true,
		Err:  -1,
	})
	if err != nil {
		handler(MultiResponse{}, err)
		return
	}
	reqBuf = append(reqBuf, endBuf...)
	conn.RequestAsync(proto.OpMulti, reqBuf, func(reply Reply) {
		if reply.Err != proto.ErrOk {
			handler(MultiResponse{},
				fmt.Errorf("Error in Multi: %v", reply.Err.Error()))
			return
		}
		replies := make([]interface{}, 0, len(ops))
		replyBuf := reply.Buf
		failed := false
		i := 0
		for {
			var header proto.MultiHeader
			replyBuf, err = jute.DecodeSome(replyBuf, &header)
			if err != nil {
				handler(MultiResponse{}, err)
				return
			}
			if header.Done {
				if len(replyBuf) > 0 {
					handler(MultiResponse{},
						fmt.Errorf("Found %v extra bytes in multi response", len(replyBuf)))
					return
				}
				if i != len(ops) {
					handler(MultiResponse{},
						fmt.Errorf("Expected %v replies to multi request, got back %v",
							len(ops), i-1))
				}
				break
			}
			var reply interface{}
			switch header.Type {
			case -1:
				failed = true
				if header.Err == proto.ErrOk || header.Err == -2 {
					replies = append(replies, proto.ErrNothing)
				} else {
					replies = append(replies, header.Err)
				}
				// discard the error code
				replyBuf, err = jute.DecodeSome(replyBuf, new(proto.MultiErrorResponse))
				if err != nil {
					handler(MultiResponse{}, err)
					return
				}
			case proto.OpCreate:
				reply = new(proto.CreateResponse)
			case proto.OpDelete:
				reply = new(proto.DeleteResponse)
			case proto.OpSetData:
				reply = new(proto.SetDataResponse)
			case proto.OpCheckVersion:
				reply = new(proto.CheckVersionResponse)
			default:
				handler(MultiResponse{},
					fmt.Errorf("Expected multi opcode in reply, found %v", header.Type))
				return
			}
			if header.Type != -1 && header.Type != ops[i].opCode {
				handler(MultiResponse{},
					fmt.Errorf("Got unexpected type (%v) in reply to multiop %v",
						header.Type, ops[i].opCode))
				return
			}
			if reply != nil {
				replyBuf, err = jute.DecodeSome(replyBuf, reply)
				if err != nil {
					handler(MultiResponse{}, err)
					return
				}
				replies = append(replies, reply)
			}
			i++
		}
		resp := MultiResponse{
			Xid:     reply.Xid,
			Zxid:    reply.Zxid,
			replies: replies,
		}
		if failed {
			handler(resp, errors.New("Multi operation was not applied"))
			return
		}
		handler(resp, nil)
	})
}

func (conn *Conn) Multi(ops ...multiOp) (MultiResponse, error) {
	type pair struct {
		resp MultiResponse
		err  error
	}
	ch := make(chan pair)
	conn.MultiAsync(ops, func(resp MultiResponse, err error) {
		ch <- pair{resp, err}
	})
	p := <-ch
	return p.resp, p.err
}

func (client *Client) Multi(ops ...multiOp) (MultiResponse, error) {
	return client.Conn().Multi(ops...)
}

func encodeMultiOp(opCode proto.OpCode, req interface{}) multiOp {
	header := proto.MultiHeader{
		Type: opCode,
		Done: false,
		Err:  -1,
	}
	headerBuf, err := jute.Encode(&header)
	if err != nil {
		return multiOp{opCode: opCode, err: err}
	}
	reqBuf, err := jute.Encode(req)
	if err != nil {
		return multiOp{opCode: opCode, err: err}
	}
	return multiOp{opCode: opCode, buf: append(headerBuf, reqBuf...)}
}

func MultiCreate(
	path proto.Path,
	data []byte,
	acl []proto.ACL,
	mode proto.CreateMode) multiOp {
	return encodeMultiOp(proto.OpCreate, &proto.CreateRequest{
		Path: path,
		Data: data,
		ACL:  acl,
		Mode: mode,
	})
}

func (mr *MultiResponse) Create(op int) (CreateResponse, error) {
	if op >= len(mr.replies) {
		return CreateResponse{}, errors.New("reply index out of range")
	}
	reply := mr.replies[op]
	switch reply := reply.(type) {
	case proto.ErrCode:
		return CreateResponse{}, reply.Error()
	case *proto.CreateResponse:
		return CreateResponse{
			Zxid: mr.Zxid,
			Path: reply.Path,
		}, nil
	}
	panic(fmt.Sprintf("Expected CreateResponse, found: %T", reply))
}

func MultiDelete(path proto.Path, version proto.Version) multiOp {
	return encodeMultiOp(proto.OpDelete, &proto.DeleteRequest{
		Path:    path,
		Version: version,
	})
}

func (mr *MultiResponse) Delete(op int) (DeleteResponse, error) {
	if op >= len(mr.replies) {
		return DeleteResponse{}, errors.New("reply index out of range")
	}
	reply := mr.replies[op]
	switch reply := reply.(type) {
	case proto.ErrCode:
		return DeleteResponse{}, reply.Error()
	case *proto.DeleteResponse:
		return DeleteResponse{
			Zxid: mr.Zxid,
		}, nil
	}
	panic(fmt.Sprintf("Expected DeleteResponse, found: %T", reply))
}

func MultiSetData(path proto.Path, data []byte, version proto.Version) multiOp {
	return encodeMultiOp(proto.OpSetData, &proto.SetDataRequest{
		Path:    path,
		Data:    data,
		Version: version,
	})
}

func (mr *MultiResponse) SetData(op int) (SetDataResponse, error) {
	if op >= len(mr.replies) {
		return SetDataResponse{}, errors.New("reply index out of range")
	}
	reply := mr.replies[op]
	switch reply := reply.(type) {
	case proto.ErrCode:
		return SetDataResponse{}, reply.Error()
	case *proto.SetDataResponse:
		return SetDataResponse{
			Zxid: mr.Zxid,
			Stat: reply.Stat,
		}, nil
	}
	panic(fmt.Sprintf("Expected SetDataResponse, found: %T", reply))
}

func MultiCheckVersion(path proto.Path, version proto.Version) multiOp {
	return encodeMultiOp(proto.OpCheckVersion, &proto.CheckVersionRequest{
		Path:    path,
		Version: version,
	})
}

func (mr *MultiResponse) CheckVersion(op int) (CheckVersionResponse, error) {
	if op >= len(mr.replies) {
		return CheckVersionResponse{}, errors.New("reply index out of range")
	}
	reply := mr.replies[op]
	switch reply := reply.(type) {
	case proto.ErrCode:
		return CheckVersionResponse{}, reply.Error()
	case *proto.CheckVersionResponse:
		return CheckVersionResponse{
			Zxid: mr.Zxid,
		}, nil
	}
	panic(fmt.Sprintf("Expected CheckVersionResponse, found: %T", reply))
}
