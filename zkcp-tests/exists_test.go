/*
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 */

package zkcptests

import (
	"strings"
	"testing"
	"time"

	"salesforce.com/zoolater/proto"
)

func TestZKCP_Exists_watchPath(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	notified := 0
	_, err := client.ExistsSync("/x/y", func(proto.EventType) {
		notified++
	})
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("Expected does not exist error, got: %v", err)
	}
	if notified > 0 {
		t.Errorf("Notified too soon")
	}
	_, err = client.CreateSync("/x", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}
	if notified > 0 {
		t.Errorf("Notified too soon")
	}
	_, err = client.CreateSync("/x/y", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}
	if notified != 1 {
		t.Errorf("Expected 1 notification before create response, got %v",
			notified)
	}
}

func TestZKCP_Exists_watchRestore(t *testing.T) {
	client := makeClient(t)
	defer client.Close()

	notified := 0
	_, err := client.ExistsSync("/x/y", func(proto.EventType) {
		notified++
	})
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("Expected does not exist error, got: %v", err)
	}
	if notified > 0 {
		t.Errorf("Notified too soon")
	}

	// Cause the server to close the connection
	client.RequestSync(proto.OpError, nil)
	time.Sleep(time.Second) // TODO: eliminate timing

	_, err = client.CreateSync("/x", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}
	if notified > 0 {
		t.Errorf("Notified too soon")
	}
	_, err = client.CreateSync("/x/y", nil, OpenACL, proto.ModeDefault)
	if err != nil {
		t.Fatal(err)
	}
	if notified != 1 {
		t.Errorf("Expected 1 notification before create response, got %v",
			notified)
	}
}
