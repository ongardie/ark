/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package statemachine

import (
	"log"

	"salesforce.com/zoolater/jute"
	"salesforce.com/zoolater/proto"
)

func applyMulti(ctx *context, tree *Tree, cmdBuf []byte) (*Tree, []byte, NotifyEvents, proto.ErrCode) {
	more := cmdBuf
	originalTree := tree
	var respHeaders []*proto.MultiHeader
	var results []interface{}
	var notify NotifyEvents
	failed := false

	for {
		opHeader := &proto.MultiHeader{}
		var err error
		more, err = jute.DecodeSome(more, opHeader)
		if err != nil {
			log.Printf("Ignoring multi request with op header decode error: %v", err)
			return nil, nil, nil, proto.ErrAPIError
		}
		if opHeader.Err != -1 {
			log.Printf("Ignoring multi request with bad op header: %v", opHeader)
			return nil, nil, nil, proto.ErrAPIError
		}
		if opHeader.Done {
			if opHeader.Type != -1 {
				log.Printf("Ignoring multi request with bad op header: %v", opHeader)
				return nil, nil, nil, proto.ErrAPIError
			}
			if len(more) > 0 {
				log.Printf("Ignoring multi request with %v extra bytes", len(more))
				return nil, nil, nil, proto.ErrAPIError
			}
			break
		}

		if failed {
			respHeaders = append(respHeaders, &proto.MultiHeader{
				Type: proto.OpError,
				Done: false,
				Err:  proto.ErrRuntimeInconsistency,
			})
		}

		switch opHeader.Type {
		case proto.OpCreate:
			log.Printf("multi create")
			op := &proto.CreateRequest{}
			more, err = jute.DecodeSome(more, op)
			if err != nil {
				log.Printf("Ignoring multi request. Could not decode create op: %v", err)
				return nil, nil, nil, proto.ErrAPIError
			}
			t, resp, n, errCode := tree.Create(ctx, op)
			notify = append(notify, n...)
			if errCode == proto.ErrOk {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpCreate,
					Done: false,
					Err:  proto.ErrOk,
				})
				results = append(results, resp)
				tree = t
			} else {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpError,
					Done: false,
					Err:  errCode,
				})
				failed = true
			}

		case proto.OpSetData:
			log.Printf("multi setData")
			op := &proto.SetDataRequest{}
			more, err = jute.DecodeSome(more, op)
			if err != nil {
				log.Printf("Ignoring multi request. Could not decode setData op: %v", err)
				return nil, nil, nil, proto.ErrAPIError
			}
			t, resp, n, errCode := tree.SetData(ctx, op)
			notify = append(notify, n...)
			if errCode == proto.ErrOk {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpSetData,
					Done: false,
					Err:  proto.ErrOk,
				})
				results = append(results, resp)
				tree = t
			} else {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpError,
					Done: false,
					Err:  errCode,
				})
				failed = true
			}

		case proto.OpDelete:
			log.Printf("multi delete")
			op := &proto.DeleteRequest{}
			more, err = jute.DecodeSome(more, op)
			if err != nil {
				log.Printf("Ignoring multi request. Could not decode delete op: %v", err)
				return nil, nil, nil, proto.ErrAPIError
			}
			t, resp, n, errCode := tree.Delete(ctx, op)
			notify = append(notify, n...)
			if errCode == proto.ErrOk {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpDelete,
					Done: false,
					Err:  proto.ErrOk,
				})
				results = append(results, resp)
				tree = t
			} else {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpError,
					Done: false,
					Err:  errCode,
				})
				failed = true
			}

		case proto.OpCheck:
			log.Printf("multi check")
			op := &proto.CheckVersionRequest{}
			more, err = jute.DecodeSome(more, op)
			if err != nil {
				log.Printf("Ignoring multi request. Could not decode check op: %v", err)
				return nil, nil, nil, proto.ErrAPIError
			}
			resp, errCode := tree.CheckVersion(ctx, op)
			if errCode == proto.ErrOk {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpCheck,
					Done: false,
					Err:  proto.ErrOk,
				})
				results = append(results, resp)
			} else {
				respHeaders = append(respHeaders, &proto.MultiHeader{
					Type: proto.OpError,
					Done: false,
					Err:  errCode,
				})
				failed = true
			}

		default:
			log.Printf("Ignoring multi request with unknown type: %v", opHeader)
			return nil, nil, nil, proto.ErrAPIError
		}
	}

	if failed {
		tree = originalTree
		notify = nil
		results = nil
		for _, hdr := range respHeaders {
			if hdr.Type != proto.OpError {
				hdr.Type = proto.OpError
			}
			results = append(results, &proto.MultiErrorResponse{hdr.Err})
		}
	}

	respHeaders = append(respHeaders, &proto.MultiHeader{
		Type: -1,
		Done: true,
		Err:  -1,
	})

	var respBuf []byte
	for i, hdr := range respHeaders {
		log.Printf("multi %+v", hdr)
		buf, err := jute.Encode(hdr)
		if err != nil {
			log.Printf("Could not encode %+v", hdr)
			return nil, nil, nil, proto.ErrAPIError
		}
		respBuf = append(respBuf, buf...)
		if len(results) > i {
			log.Printf("multi %+v", results[i])
			buf, err := jute.Encode(results[i])
			if err != nil {
				log.Printf("Could not encode %+v", results[i])
				return nil, nil, nil, proto.ErrAPIError
			}
			respBuf = append(respBuf, buf...)
		}
	}

	return tree, respBuf, notify, proto.ErrOk
}
