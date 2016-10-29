/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package main

import (
	zk "salesforce.com/zoolater/client"
	"salesforce.com/zoolater/proto"
)

// Tests the behavior of czxid, ctime in the Stat struct.
func (t *Test) TestZKCP_Stat_create() {
	client := makeClient(t)
	defer client.Close()
	start := zknow()

	// Create a new node.
	cresp, err := client.Create("/x", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}
	czxid := cresp.Zxid
	if czxid <= 0 {
		t.Errorf("Bad zxid: %v", czxid)
	}

	// Check its czxid, ctime.
	sresp, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp.Stat.Czxid != czxid {
		t.Errorf("stat czxid (%v) does not match create czxid (%v)",
			sresp.Stat.Czxid, czxid)
	}
	if sresp.Stat.Ctime < start-ClockSkew || sresp.Stat.Ctime > start+ClockSkew {
		t.Errorf("stat ctime (%v) bad, expected %v",
			sresp.Stat.Ctime, start)
	}

	// Change irrelevant things.
	err = changeAll(client, "/x")
	if err != nil {
		t.Error(err)
	}

	// Check its czxid, ctime (MUST be unchanged).
	sresp2, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp2.Stat.Czxid != czxid {
		t.Errorf("stat czxid (%v) no longer matches create czxid (%v)",
			sresp2.Stat.Czxid, czxid)
	}
	if sresp2.Stat.Ctime != sresp.Stat.Ctime {
		t.Errorf("stat ctime (%v) no longer matches create ctime (%v)",
			sresp2.Stat.Ctime, sresp.Stat.Ctime)
	}
}

// Tests the behavior of mzxid, version, mtime in the Stat struct.
func (t *Test) TestZKCP_Stat_mod() {
	client := makeClient(t)
	defer client.Close()
	start := zknow()

	// Create a new node.
	_, err := client.Create("/x", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}

	// Check its mzxid, version, mtime.
	sresp, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp.Stat.Mzxid != sresp.Stat.Czxid {
		t.Errorf("stat mzxid (%v) does not match create czxid (%v)",
			sresp.Stat.Mzxid, sresp.Stat.Czxid)
	}
	if sresp.Stat.Version != 0 {
		t.Errorf("version (%v) must be 0",
			sresp.Stat.Version)
	}
	if sresp.Stat.Mtime < start-ClockSkew || sresp.Stat.Mtime > start+ClockSkew {
		t.Errorf("mtime (%v) bad for new znode, expected %v",
			sresp.Stat.Mtime, start)
	}
	if sresp.Stat.Mtime != sresp.Stat.Ctime {
		t.Errorf("mtime (%v) must equal ctime (%v) for new znode",
			sresp.Stat.Mtime, sresp.Stat.Ctime)
	}

	// Change irrelevant things.
	err = changeACL(client, "/x")
	if err != nil {
		t.Error(err)
	}
	err = changeChildren(client, "/x")
	if err != nil {
		t.Error(err)
	}

	// Check its mzxid, version, mtime (MUST be unchanged).
	sresp2, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp2.Stat.Mzxid != sresp2.Stat.Czxid {
		t.Errorf("stat mzxid (%v) no longer matches create czxid (%v)",
			sresp2.Stat.Mzxid, sresp2.Stat.Czxid)
	}
	if sresp2.Stat.Version != 0 {
		t.Errorf("version (%v) is no longer 0",
			sresp2.Stat.Version)
	}
	if sresp2.Stat.Mtime != sresp.Stat.Mtime {
		t.Errorf("mtime (%v) is no longer %v",
			sresp2.Stat.Mtime, sresp.Stat.Mtime)
	}

	// Setting the data MUST change mzxid, version, mtime.
	dresp, err := client.SetData("/x", []byte("hi"), -1)
	if err != nil {
		t.Fatal(err)
	}
	if dresp.Stat.Mzxid != dresp.Zxid {
		t.Errorf("stat mzxid (%v) doesn't match setData zxid (%v)",
			dresp.Stat.Mzxid, dresp.Zxid)
	}
	if dresp.Stat.Version != 1 {
		t.Errorf("version (%v) must be 1",
			dresp.Stat.Version)
	}
	if dresp.Stat.Mtime < sresp.Stat.Mtime {
		t.Errorf("mtime (%v) must be higher than %v after setData",
			dresp.Stat.Mtime, sresp.Stat.Mtime)
	}
	if dresp.Stat.Mtime > start+ClockSkew {
		t.Errorf("mtime (%v) too high after setData, started test at %v",
			dresp.Stat.Mtime, start)
	}

	// Setting the data to the same thing MUST still change mzxid, version, mtime.
	dresp2, err := client.SetData("/x", []byte("hi"), -1)
	if err != nil {
		t.Fatal(err)
	}
	if dresp2.Zxid <= dresp.Zxid {
		t.Errorf("duplicate setData zxid (%v) < original setData zxid (%v)",
			dresp2.Zxid, dresp.Zxid)
	}
	if dresp2.Stat.Mzxid != dresp2.Zxid {
		t.Errorf("stat mzxid (%v) doesn't match duplicate setData zxid (%v)",
			dresp2.Stat.Mzxid, dresp2.Zxid)
	}
	if dresp2.Stat.Version != 2 {
		t.Errorf("version (%v) must be 2",
			dresp2.Stat.Version)
	}
	if dresp2.Stat.Mtime < dresp.Stat.Mtime {
		t.Errorf("mtime (%v) must be higher than %v after duplicate setData",
			dresp2.Stat.Mtime, dresp.Stat.Mtime)
	}
	if dresp2.Stat.Mtime > start+ClockSkew {
		t.Errorf("mtime (%v) too high after duplicate setData, started test at %v",
			dresp2.Stat.Mtime, start)
	}
}

// Test the behavior of version for multi-ops.
func (t *Test) TestZKCP_Stat_version_multi() {
	client := makeClient(t)
	defer client.Close()

	// Create a new node.
	_, err := client.Create("/x", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}

	// Change it twice in one op.
	_, err = client.Multi(
		zk.MultiSetData("/x", []byte("v1"), -1),
		zk.MultiSetData("/x", []byte("v2"), -1))
	if err != nil {
		t.Fatal(err)
	}

	// Check its version.
	sresp, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp.Stat.Version != 2 {
		t.Errorf("Expected version 2, got %v",
			sresp.Stat.Version)
	}
}

// Tests the behavior of cversion, numChildren, pzxid in the Stat struct.
func (t *Test) TestZKCP_Stat_children() {
	client := makeClient(t)
	defer client.Close()

	// Create a new node.
	_, err := client.Create("/x", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}

	// Check its cverson, numChildren, pzxid.
	sresp, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp.Stat.Cversion != 0 {
		t.Errorf("cversion (%v) must be 0 for new znode",
			sresp.Stat.Cversion)
	}
	if sresp.Stat.NumChildren != 0 {
		t.Errorf("numChildren (%v) must be 0 for new znode",
			sresp.Stat.NumChildren)
	}
	if sresp.Stat.Pzxid != sresp.Stat.Czxid {
		t.Errorf("pzxid (%v) must be czxid (%v) for new znode",
			sresp.Stat.Pzxid, sresp.Stat.Czxid)
	}

	// Change irrelevant things.
	err = changeACL(client, "/x")
	if err != nil {
		t.Error(err)
	}
	err = changeData(client, "/x")
	if err != nil {
		t.Error(err)
	}

	// Check its cversion, numChildren, pzxid (MUST be unchanged).
	sresp2, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp2.Stat.Cversion != 0 {
		t.Errorf("cversion (%v) is no longer 0",
			sresp2.Stat.Cversion)
	}
	if sresp2.Stat.NumChildren != 0 {
		t.Errorf("numChildren (%v) is no longer 0",
			sresp2.Stat.NumChildren)
	}
	if sresp2.Stat.Pzxid != sresp2.Stat.Czxid {
		t.Errorf("pzxid (%v) no longer matches czxid (%v)",
			sresp2.Stat.Pzxid, sresp2.Stat.Czxid)
	}

	// Creating a child node MUST change cversion, numChildren, pzxid.
	cresp, err := client.Create("/x/1", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}
	sresp3, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp3.Stat.Cversion != 1 {
		t.Errorf("cversion (%v) must be 1 after creating child",
			sresp2.Stat.Cversion)
	}
	if sresp3.Stat.NumChildren != 1 {
		t.Errorf("numChildren (%v) must be 1 after creating child",
			sresp2.Stat.NumChildren)
	}
	if sresp3.Stat.Pzxid != cresp.Zxid {
		t.Errorf("pzxid (%v) must be zxid of child create request (%v)",
			sresp3.Stat.Pzxid, cresp.Zxid)
	}

	// Creating or deleting a child of /x/1 MUST make no difference.
	_, err = client.Create("/x/1/2", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}
	sresp4, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp4.Stat != sresp3.Stat {
		t.Errorf("stat (%+v) must not change after creating indirect child, was %+v",
			sresp4.Stat, sresp3.Stat)
	}
	_, err = client.Delete("/x/1/2", -1)
	if err != nil {
		t.Fatal(err)
	}
	sresp5, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp5.Stat != sresp3.Stat {
		t.Errorf("stat (%+v) must not change after deleting indirect child, was %+v",
			sresp5.Stat, sresp3.Stat)
	}

	// Deleting a child node MUST change cversion, numChildren, pzxid.
	dresp, err := client.Delete("/x/1", -1)
	if err != nil {
		t.Fatal(err)
	}
	sresp6, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp6.Stat.Cversion != 2 {
		t.Errorf("cversion (%v) must be 2 after deleting child",
			sresp6.Stat.Cversion)
	}
	if sresp6.Stat.NumChildren != 0 {
		t.Errorf("numChildren (%v) must be 0 after deleting child",
			sresp6.Stat.NumChildren)
	}
	if sresp6.Stat.Pzxid != dresp.Zxid {
		t.Errorf("pzxid (%v) must be zxid of child delete request (%v)",
			sresp6.Stat.Pzxid, dresp.Zxid)
	}
}

// Test the behavior of cversion for multi-ops.
func (t *Test) TestZKCP_Stat_cversion_multi() {
	client := makeClient(t)
	defer client.Close()

	// Create a new node.
	_, err := client.Create("/x", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}

	// Add two children and remove one in one multi-op.
	_, err = client.Multi(
		zk.MultiCreate("/x/1", nil, OpenACL, proto.ModeDefault),
		zk.MultiCreate("/x/2", nil, OpenACL, proto.ModeDefault),
		zk.MultiDelete("/x/1", -1))
	if err != nil {
		t.Fatal(err)
	}

	// Check its cversion.
	sresp, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp.Stat.Cversion != 3 {
		t.Errorf("Expected cversion 3, got %v",
			sresp.Stat.Version)
	}
}

// Tests the behavior of dataLength in the Stat struct.
func (t *Test) TestZKCP_Stat_dataLength() {
	client := makeClient(t)
	defer client.Close()

	// Create a new node.
	_, err := client.Create("/x", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}
	sresp, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp.Stat.DataLength != 0 {
		t.Errorf("dataLength (%v) must be 0",
			sresp.Stat.DataLength)
	}

	// Set 2 bytes of data.
	dresp, err := client.SetData("/x", []byte("hi"), -1)
	if err != nil {
		t.Fatal(err)
	}
	if dresp.Stat.DataLength != 2 {
		t.Errorf("dataLength (%v) must be 2 after setData",
			dresp.Stat.DataLength)
	}

	// Create another new node with data.
	_, err = client.Create("/y", []byte("hello"), OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}
	sresp2, err := client.Exists("/y", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp2.Stat.DataLength != 5 {
		t.Errorf("dataLength (%v) must be 5 for create with data",
			sresp2.Stat.DataLength)
	}

}

// Tests the behavior of aversion in the Stat struct.
func (t *Test) TestZKCP_Stat_acl() {
	client := makeClient(t)
	defer client.Close()

	// Create a new node.
	_, err := client.Create("/x", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}

	// Check its aversion.
	sresp, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp.Stat.Aversion != 0 {
		t.Errorf("version (%v) must be 0",
			sresp.Stat.Aversion)
	}

	// Change irrelevant things.
	err = changeData(client, "/x")
	if err != nil {
		t.Error(err)
	}
	err = changeChildren(client, "/x")
	if err != nil {
		t.Error(err)
	}

	// Check its aversion (MUST be unchanged).
	sresp2, err := client.Exists("/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if sresp2.Stat.Aversion != 0 {
		t.Errorf("aversion (%v) is no longer 0",
			sresp2.Stat.Version)
	}

	// Setting the ACL MUST change aversion.
	aresp, err := client.SetACL("/x",
		[]proto.ACL{{proto.PermAdmin | proto.PermDelete | proto.PermRead, Anyone}},
		-1)
	if err != nil {
		t.Fatal(err)
	}
	if aresp.Stat.Aversion != 1 {
		t.Errorf("aversion (%v) must be 1",
			aresp.Stat.Aversion)
	}

	// Setting the ACL to the same thing MUST still change aversion.
	aresp2, err := client.SetACL("/x",
		[]proto.ACL{{proto.PermAdmin | proto.PermDelete | proto.PermRead, Anyone}},
		-1)
	if err != nil {
		t.Fatal(err)
	}
	if aresp2.Stat.Aversion != 2 {
		t.Errorf("aversion (%v) must be 2",
			aresp2.Stat.Aversion)
	}
}

// Tests the behavior of ephemeralOwner in the Stat struct.
func (t *Test) TestZKCP_Stat_ephemeral() {
	client := makeClient(t)
	defer client.Close()
	sessionId, _ := client.Session()

	// Create a new node.
	crespDefault, err := client.Create2(
		"/default",
		nil,
		OpenACL,
		proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}
	if crespDefault.Stat.EphemeralOwner != 0 {
		t.Errorf("epehemeral owner (%v) must be 0 for ModeDefault",
			crespDefault.Stat.EphemeralOwner)
	}

	crespEphemeral, err := client.Create2(
		"/ephemeral",
		nil,
		OpenACL,
		proto.ModeEphemeral)
	if err != nil {
		t.Fatal(err)
	}
	if crespEphemeral.Stat.EphemeralOwner != sessionId {
		t.Errorf("epehemeral owner (%v) must be session id (%v) for ModeEphemeral",
			crespEphemeral.Stat.EphemeralOwner, sessionId)
	}

	crespSequential, err := client.Create2(
		"/sequential",
		nil,
		OpenACL,
		proto.ModeSequential)
	if err != nil {
		t.Fatal(err)
	}
	if crespSequential.Stat.EphemeralOwner != 0 {
		t.Errorf("epehemeral owner (%v) must be 0 for ModeSequential",
			crespSequential.Stat.EphemeralOwner)
	}

	crespEphemeralSeq, err := client.Create2(
		"/ephemeral-sequential",
		nil,
		OpenACL,
		proto.ModeEphemeralSequential)
	if err != nil {
		t.Fatal(err)
	}
	if crespEphemeralSeq.Stat.EphemeralOwner != sessionId {
		t.Errorf("epehemeral owner (%v) must be session id (%v) for ModeEphemeralSequential",
			crespEphemeralSeq.Stat.EphemeralOwner, sessionId)
	}

	crespContainer, err := client.Create2(
		"/container",
		nil,
		OpenACL,
		proto.ModeContainer)
	if err != nil {
		t.Fatal(err)
	}
	if crespContainer.Stat.EphemeralOwner != 0 {
		t.Errorf("epehemeral owner (%v) must be 0 for ModeContainer",
			crespContainer.Stat.EphemeralOwner)
	}
}
