/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"sort"
	"strings"
)

type Context struct {
	zxid int64
	time int64
}

type Tree struct {
	data     []byte
	acl      []ACL
	stat     Stat
	children map[Component]*Tree
}

func NewTree() *Tree {
	return &Tree{}
}

func splitPath(path Path) []Component {
	split := strings.Split(string(path), "/")
	components := make([]Component, 0, len(split))
	for _, c := range split {
		if len(c) == 0 {
			continue
		}
		components = append(components, Component(c))
	}
	return components
}

func (t *Tree) shallowClone() *Tree {
	tmp := *t
	return &tmp
}

func (old *Tree) withChild(name Component, child *Tree) *Tree {
	node := old.shallowClone()
	node.children = make(map[Component]*Tree, len(old.children))
	for k, v := range old.children {
		node.children[k] = v
	}
	node.children[name] = child
	return node
}

// TODO: req.Flags
func (t *Tree) Create(ctx *Context, req *CreateRequest) (*Tree, *createResponse, error) {
	var do func(node *Tree, components []Component) (*Tree, error)
	do = func(node *Tree, components []Component) (*Tree, error) {
		if len(components) == 1 {
			_, ok := node.children[components[0]]
			if ok {
				return nil, ErrNodeExists
			}
			return node.withChild(components[0], &Tree{
				data: req.Data,
				acl:  req.Acl,
				stat: Stat{
					Czxid: ctx.zxid,
					Ctime: ctx.time,
				},
			}), nil
		} else {
			child, ok := node.children[components[0]]
			if !ok {
				return nil, ErrNoNode
			}
			newChild, err := do(child, components[1:])
			if err != nil {
				return nil, err
			}
			return node.withChild(components[0], newChild), nil
		}
	}

	components := splitPath(req.Path)
	if len(components) == 0 {
		return nil, nil, ErrNodeExists
	}
	root, err := do(t, components)
	if err != nil {
		return nil, nil, err
	}
	return root, &createResponse{
		Path: req.Path,
	}, nil
}

type ComponentSortable []Component

func (p ComponentSortable) Len() int           { return len(p) }
func (p ComponentSortable) Less(i, j int) bool { return p[i] < p[j] }
func (p ComponentSortable) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// TODO: req.Watch
func (t *Tree) GetChildren(ctx *Context, req *getChildren2Request) (*Tree, *getChildren2Response, error) {
	resp := &getChildren2Response{}
	var do func(node *Tree, components []Component) error
	do = func(node *Tree, components []Component) error {
		if len(components) == 0 {
			resp.Children = make([]Component, 0, len(node.children))
			for name := range node.children {
				resp.Children = append(resp.Children, name)
			}
			sort.Sort(ComponentSortable(resp.Children))
			resp.Stat = node.stat
			return nil
		} else {
			child, ok := node.children[components[0]]
			if !ok {
				return ErrNoNode
			}
			return do(child, components[1:])
		}
	}
	err := do(t, splitPath(req.Path))
	if err != nil {
		return t, nil, err
	}
	return t, resp, nil
}

// TODO: Watch
func (t *Tree) GetData(ctx *Context, req *getDataRequest) (*Tree, *getDataResponse, error) {
	resp := &getDataResponse{}
	var do func(node *Tree, components []Component) error
	do = func(node *Tree, components []Component) error {
		if len(components) == 0 {
			resp.Data = node.data
			resp.Stat = node.stat
			return nil
		} else {
			child, ok := node.children[components[0]]
			if !ok {
				return ErrNoNode
			}
			return do(child, components[1:])
		}
	}
	err := do(t, splitPath(req.Path))
	if err != nil {
		return t, nil, err
	}
	return t, resp, nil
}

// TODO: Version
func (t *Tree) SetData(ctx *Context, req *SetDataRequest) (*Tree, *setDataResponse, error) {
	resp := &setDataResponse{}
	var do func(node *Tree, components []Component) (*Tree, error)
	do = func(node *Tree, components []Component) (*Tree, error) {
		if len(components) == 0 {
			if req.Version >= 0 && node.stat.Version != req.Version {
				return nil, ErrBadVersion
			}
			node = node.shallowClone()
			node.data = req.Data
			node.stat.Mzxid = ctx.zxid
			node.stat.Mtime = ctx.time
			node.stat.Version += 1 // TODO: overflow?
			resp.Stat = node.stat
			return node, nil
		} else {
			child, ok := node.children[components[0]]
			if !ok {
				return nil, ErrNoNode
			}
			newChild, err := do(child, components[1:])
			if err != nil {
				return nil, err
			}
			return node.withChild(components[0], newChild), nil
		}
	}
	root, err := do(t, splitPath(req.Path))
	if err != nil {
		return nil, nil, err
	}
	return root, resp, nil
}
