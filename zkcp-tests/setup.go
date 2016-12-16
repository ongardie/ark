/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	"math/rand"
	"strings"
	"time"

	"salesforce.com/ark/client"
	"salesforce.com/ark/proto"
)

const ClockSkew = 60000

var OpenACL = []proto.ACL{{proto.PermAll, Anyone}}
var Anyone = proto.Identity{Scheme: "world", ID: "anyone"}

func randLetter() string {
	return string(byte('a') + byte(rand.Intn(26)))
}

func join(p proto.Path, c proto.Component) proto.Path {
	if p == "/" {
		return proto.Path("/" + string(c))
	}
	return proto.Path(string(p) + "/" + string(c))
}

func zknow() proto.Time {
	return proto.Time(time.Now().UnixNano() / 1e6)
}

func makeClient(t *Test) *client.Client {
	client := client.New([]string{"localhost:2181"}, nil)
	err := deleteAll(client, "/")
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func deleteAll(client *client.Client, path proto.Path) error {
	if path == "/zookeeper" || strings.HasPrefix(string(path), "/zookeeper/") {
		return nil
	}
	resp, err := client.GetChildren(path, nil)
	if err != nil {
		return err
	}
	for _, child := range resp.Children {
		err := deleteAll(client, join(path, child))
		if err != nil {
			return err
		}
	}
	if path != "/" {
		_, err = client.Delete(path, -1)
	}
	return err
}

func changeAll(client *client.Client, path proto.Path) error {
	err := changeData(client, path)
	if err != nil {
		return err
	}
	err = changeACL(client, path)
	if err != nil {
		return err
	}
	err = changeChildren(client, path)
	if err != nil {
		return err
	}
	return nil
}

func changeData(client *client.Client, path proto.Path) error {
	gresp, err := client.GetData(path, nil)
	if err != nil {
		return err
	}
	data := append(gresp.Data, randLetter()...)
	_, err = client.SetData(path, data, -1)
	return err
}

func changeACL(client *client.Client, path proto.Path) error {
	gresp, err := client.GetACL(path)
	if err != nil {
		return err
	}
	acl := append(gresp.ACL, proto.ACL{proto.PermAll, Anyone})
	_, err = client.SetACL(path, acl, -1)
	return err
}

func changeChildren(client *client.Client, path proto.Path) error {
	var newChild proto.Component
	for {
		newChild += proto.Component(randLetter())
		_, err := client.Create(
			join(path, newChild),
			nil,
			[]proto.ACL{{proto.PermAll, Anyone}},
			proto.ModeDefault)
		if err != nil {
			// TODO: test exists
			return err
		}
		return nil
	}
}
