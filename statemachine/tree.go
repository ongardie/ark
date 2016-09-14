/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package statemachine

import (
	"sort"
	"strings"

	"salesforce.com/zoolater/proto"
)

type Tree struct {
	data     []byte
	acl      []proto.ACL
	stat     proto.Stat
	children map[proto.Component]*Tree
}

func NewTree() *Tree {
	return &Tree{}
}

func splitPath(path proto.Path) []proto.Component {
	split := strings.Split(string(path), "/")
	components := make([]proto.Component, 0, len(split))
	for _, c := range split {
		if len(c) == 0 {
			continue
		}
		components = append(components, proto.Component(c))
	}
	return components
}

func joinPath(components []proto.Component) proto.Path {
	ss := make([]string, 0, len(components))
	for _, c := range components {
		ss = append(ss, string(c))
	}
	return "/" + proto.Path(strings.Join(ss, "/"))
}

func (t *Tree) shallowClone() *Tree {
	tmp := *t
	return &tmp
}

func (old *Tree) withChild(name proto.Component, child *Tree) *Tree {
	node := old.withoutChild(name)
	node.children[name] = child
	return node
}

func (old *Tree) withoutChild(name proto.Component) *Tree {
	node := old.shallowClone()
	node.children = make(map[proto.Component]*Tree, len(old.children))
	for k, v := range old.children {
		if k != name {
			node.children[k] = v
		}
	}
	return node
}

func (t *Tree) lookup(path proto.Path) *Tree {
	components := splitPath(path)
	node := t
	for _, component := range components {
		var ok bool
		node, ok = node.children[component]
		if !ok {
			return nil
		}
	}
	return node
}

// TODO: req.Flags
func (t *Tree) Create(ctx *context, req *proto.CreateRequest) (*Tree, *proto.CreateResponse, NotifyEvents, proto.ErrCode) {
	var notify NotifyEvents
	var do func(node *Tree, components []proto.Component) (*Tree, proto.ErrCode)
	do = func(node *Tree, components []proto.Component) (*Tree, proto.ErrCode) {
		if len(components) == 1 {
			_, ok := node.children[components[0]]
			if ok {
				return nil, proto.ErrNodeExists
			}
			notify = append(notify,
				TreeEvent{req.Path, proto.EventNodeCreated},
				TreeEvent{joinPath(components[:len(components)-1]), proto.EventNodeChildrenChanged})
			node = node.withChild(components[0], &Tree{
				data: req.Data,
				acl:  req.Acl,
				stat: proto.Stat{
					Czxid: ctx.zxid,
					Ctime: ctx.time,
				},
			})
			node.stat.Pzxid = ctx.zxid
			node.stat.Cversion += 1 // TODO: overflow?
			node.stat.NumChildren++
			return node, proto.ErrOk
		} else {
			child, ok := node.children[components[0]]
			if !ok {
				return nil, proto.ErrNoNode
			}
			newChild, err := do(child, components[1:])
			if err != proto.ErrOk {
				return nil, err
			}
			return node.withChild(components[0], newChild), proto.ErrOk
		}
	}

	components := splitPath(req.Path)
	if len(components) == 0 {
		return nil, nil, nil, proto.ErrNodeExists
	}
	root, err := do(t, components)
	if err != proto.ErrOk {
		return nil, nil, nil, err
	}
	return root, &proto.CreateResponse{
		Path: req.Path,
	}, notify, proto.ErrOk
}

func (t *Tree) Delete(ctx *context, req *proto.DeleteRequest) (*Tree, *proto.DeleteResponse, NotifyEvents, proto.ErrCode) {
	var notify NotifyEvents
	var do func(node *Tree, components []proto.Component) (*Tree, proto.ErrCode)
	do = func(node *Tree, components []proto.Component) (*Tree, proto.ErrCode) {
		if len(components) == 1 {
			target, ok := node.children[components[0]]
			if !ok {
				return nil, proto.ErrNoNode
			}
			if req.Version >= 0 && target.stat.Version != req.Version {
				return nil, proto.ErrBadVersion
			}
			if len(target.children) > 0 {
				return nil, proto.ErrNotEmpty
			}
			notify = append(notify,
				TreeEvent{req.Path, proto.EventNodeDeleted},
				TreeEvent{joinPath(components[:len(components)-1]), proto.EventNodeChildrenChanged})
			node = node.withoutChild(components[0])
			node.stat.Pzxid = ctx.zxid
			node.stat.Cversion += 1 // TODO: overflow?
			node.stat.NumChildren--
			return node, proto.ErrOk
		} else {
			child, ok := node.children[components[0]]
			if !ok {
				return nil, proto.ErrNoNode
			}
			newChild, err := do(child, components[1:])
			if err != proto.ErrOk {
				return nil, err
			}
			return node.withChild(components[0], newChild), proto.ErrOk
		}
	}

	components := splitPath(req.Path)
	if len(components) == 0 {
		return nil, nil, nil, proto.ErrBadArguments
	}
	root, err := do(t, components)
	if err != proto.ErrOk {
		return nil, nil, nil, err
	}
	return root, &proto.DeleteResponse{}, notify, proto.ErrOk
}

// This one is a little funny in that it can return RegisterEvents even with proto.ErrNoNode.
func (t *Tree) Exists(ctx *context, req *proto.ExistsRequest) (*proto.ExistsResponse, RegisterEvents, proto.ErrCode) {
	var register RegisterEvents
	target := t.lookup(req.Path)
	if target == nil {
		if req.Watch {
			register = append(register,
				TreeEvent{req.Path, proto.EventNodeCreated})
		}
		return nil, register, proto.ErrNoNode
	}
	resp := &proto.ExistsResponse{
		Stat: target.stat,
	}
	if req.Watch {
		register = append(register,
			TreeEvent{req.Path, proto.EventNodeDataChanged},
			TreeEvent{req.Path, proto.EventNodeDeleted})
	}
	return resp, register, proto.ErrOk
}

type ComponentSortable []proto.Component

func (p ComponentSortable) Len() int           { return len(p) }
func (p ComponentSortable) Less(i, j int) bool { return p[i] < p[j] }
func (p ComponentSortable) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (t *Tree) GetChildren2(ctx *context, req *proto.GetChildren2Request) (*proto.GetChildren2Response, RegisterEvents, proto.ErrCode) {
	var register RegisterEvents
	target := t.lookup(req.Path)
	if target == nil {
		return nil, register, proto.ErrNoNode
	}
	resp := &proto.GetChildren2Response{}
	resp.Children = make([]proto.Component, 0, len(target.children))
	for name := range target.children {
		resp.Children = append(resp.Children, name)
	}
	sort.Sort(ComponentSortable(resp.Children))
	resp.Stat = target.stat
	if req.Watch {
		register = append(register,
			TreeEvent{req.Path, proto.EventNodeChildrenChanged},
			TreeEvent{req.Path, proto.EventNodeDeleted})
	}
	return resp, register, proto.ErrOk
}

func (t *Tree) GetData(ctx *context, req *proto.GetDataRequest) (*proto.GetDataResponse, RegisterEvents, proto.ErrCode) {
	var register RegisterEvents
	target := t.lookup(req.Path)
	if target == nil {
		return nil, register, proto.ErrNoNode
	}
	resp := &proto.GetDataResponse{
		Data: target.data,
		Stat: target.stat,
	}
	if req.Watch {
		register = append(register,
			TreeEvent{req.Path, proto.EventNodeDataChanged},
			TreeEvent{req.Path, proto.EventNodeDeleted})
	}
	return resp, register, proto.ErrOk
}

func (t *Tree) SetData(ctx *context, req *proto.SetDataRequest) (*Tree, *proto.SetDataResponse, NotifyEvents, proto.ErrCode) {
	var notify NotifyEvents
	resp := &proto.SetDataResponse{}
	var do func(node *Tree, components []proto.Component) (*Tree, proto.ErrCode)
	do = func(node *Tree, components []proto.Component) (*Tree, proto.ErrCode) {
		if len(components) == 0 {
			if req.Version >= 0 && node.stat.Version != req.Version {
				return nil, proto.ErrBadVersion
			}
			node = node.shallowClone()
			node.data = req.Data
			node.stat.Mzxid = ctx.zxid
			node.stat.Mtime = ctx.time
			node.stat.Version += 1 // TODO: overflow?
			resp.Stat = node.stat
			notify = append(notify,
				TreeEvent{req.Path, proto.EventNodeDataChanged})
			return node, proto.ErrOk
		} else {
			child, ok := node.children[components[0]]
			if !ok {
				return nil, proto.ErrNoNode
			}
			newChild, err := do(child, components[1:])
			if err != proto.ErrOk {
				return nil, err
			}
			return node.withChild(components[0], newChild), proto.ErrOk
		}
	}
	root, err := do(t, splitPath(req.Path))
	if err != proto.ErrOk {
		return nil, nil, nil, err
	}
	return root, resp, notify, proto.ErrOk
}

// This behaves mostly like a query, except that it can also send inferred
// watch notifications to the connection that invokes it.
func (t *Tree) SetWatches(ctx *context, req *proto.SetWatchesRequest) (*proto.SetWatchesResponse, RegisterEvents, NotifyEvents, proto.ErrCode) {
	var register RegisterEvents
	var notify NotifyEvents
	resp := &proto.SetWatchesResponse{}

	for _, path := range req.DataWatches {
		target := t.lookup(path)
		if target == nil {
			notify = append(notify,
				TreeEvent{path, proto.EventNodeDeleted})
		} else if target.stat.Mzxid > req.RelativeZxid {
			notify = append(notify,
				TreeEvent{path, proto.EventNodeDataChanged})
		} else {
			register = append(register,
				TreeEvent{path, proto.EventNodeDataChanged},
				TreeEvent{path, proto.EventNodeDeleted})
		}
	}

	for _, path := range req.ExistWatches {
		target := t.lookup(path)
		if target != nil {
			notify = append(notify,
				TreeEvent{path, proto.EventNodeCreated})
		} else {
			register = append(register,
				TreeEvent{path, proto.EventNodeCreated})
		}
	}

	for _, path := range req.ChildWatches {
		target := t.lookup(path)
		if target == nil {
			notify = append(notify,
				TreeEvent{path, proto.EventNodeDeleted})
		} else if target.stat.Pzxid > req.RelativeZxid {
			notify = append(notify,
				TreeEvent{path, proto.EventNodeChildrenChanged})
		} else {
			register = append(register,
				TreeEvent{path, proto.EventNodeChildrenChanged},
				TreeEvent{path, proto.EventNodeDeleted})
		}
	}

	return resp, register, notify, proto.ErrOk
}
