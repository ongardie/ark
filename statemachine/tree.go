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
			return node.withChild(components[0], &Tree{
				data: req.Data,
				acl:  req.Acl,
				stat: proto.Stat{
					Czxid: ctx.zxid,
					Ctime: ctx.time,
				},
			}), proto.ErrOk
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
			return node.withoutChild(components[0]), proto.ErrOk
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

type ComponentSortable []proto.Component

func (p ComponentSortable) Len() int           { return len(p) }
func (p ComponentSortable) Less(i, j int) bool { return p[i] < p[j] }
func (p ComponentSortable) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (t *Tree) GetChildren2(ctx *context, req *proto.GetChildren2Request) (*proto.GetChildren2Response, RegisterEvents, proto.ErrCode) {
	var register RegisterEvents
	resp := &proto.GetChildren2Response{}
	var do func(node *Tree, components []proto.Component) proto.ErrCode
	do = func(node *Tree, components []proto.Component) proto.ErrCode {
		if len(components) == 0 {
			resp.Children = make([]proto.Component, 0, len(node.children))
			for name := range node.children {
				resp.Children = append(resp.Children, name)
			}
			sort.Sort(ComponentSortable(resp.Children))
			resp.Stat = node.stat
			if req.Watch {
				register = append(register,
					TreeEvent{req.Path, proto.EventNodeChildrenChanged},
					TreeEvent{req.Path, proto.EventNodeDeleted})
			}
			return proto.ErrOk
		} else {
			child, ok := node.children[components[0]]
			if !ok {
				return proto.ErrNoNode
			}
			return do(child, components[1:])
		}
	}
	err := do(t, splitPath(req.Path))
	if err != proto.ErrOk {
		return nil, nil, err
	}
	return resp, register, proto.ErrOk
}

func (t *Tree) GetData(ctx *context, req *proto.GetDataRequest) (*proto.GetDataResponse, RegisterEvents, proto.ErrCode) {
	var register RegisterEvents
	resp := &proto.GetDataResponse{}
	var do func(node *Tree, components []proto.Component) proto.ErrCode
	do = func(node *Tree, components []proto.Component) proto.ErrCode {
		if len(components) == 0 {
			resp.Data = node.data
			resp.Stat = node.stat
			if req.Watch {
				register = append(register,
					TreeEvent{req.Path, proto.EventNodeDataChanged},
					TreeEvent{req.Path, proto.EventNodeDeleted})
			}
			return proto.ErrOk
		} else {
			child, ok := node.children[components[0]]
			if !ok {
				return proto.ErrNoNode
			}
			return do(child, components[1:])
		}
	}
	err := do(t, splitPath(req.Path))
	if err != proto.ErrOk {
		return nil, nil, err
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
