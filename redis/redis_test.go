// Copyright 2013 Alexandre Fiori
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package redis

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"
)

// rc is the redis client handler used for all tests.
// Make sure redis-server is running before starting the tests.
var rc *Client

func init() {
	rc = New("127.0.0.1:6379")
	rand.Seed(time.Now().UTC().UnixNano())
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func errMsg(msg interface{}) string {
	return fmt.Sprintf("Unexpected response from redis-server: %#v\n", msg)
}

// Tests

// TestAppend appends " World" to "Hello" and expects the lenght to be 11.
func _TestAppend(t *testing.T) {
	n, err := rc.Append("foobar", "Hello")
	if err != nil {
		t.Error(err)
		return
	}
	n, err = rc.Append("foobar", " World")
	if err != nil {
		t.Error(err)
		goto cleanup
	}
	if n != 11 {
		t.Error(errMsg(n))
	}
cleanup:
	rc.Delete("foobar")
}

// TestBgRewriteAOF starts an Append Only File rewrite process.
func _TestBgRewriteAOF(t *testing.T) {
	status, err := rc.BgRewriteAOF()
	if err != nil {
		t.Error(err)
	} else if status != "Background append only file rewriting started" {
		t.Error(errMsg(status))
	}
}

// TestBgSave saves the DB in background.
func _TestBgSave(t *testing.T) {
	status, err := rc.BgSave()
	if err != nil {
		t.Error(err)
	} else if status != "Background saving started" {
		t.Error(errMsg(status))
	}
}

// TestBitCount reproduces the example from http://redis.io/commands/bitcount.
func _TestBitCount(t *testing.T) {
	err := rc.Set("mykey", "foobar")
	if err != nil {
		t.Error(err)
		return
	}
	n, err := rc.BitCount("mykey", -1, -1)
	if err != nil {
		t.Error(err)
		goto cleanup
	}
	if n != 26 {
		t.Error(errMsg(n))
	}
cleanup:
	rc.Delete("foobar")
}

// TestBitOp reproduces the example from http://redis.io/commands/bitop.
func _TestBitOp(t *testing.T) {
	err := rc.Set("key1", "foobar")
	if err != nil {
		t.Error(err)
		return
	}
	err = rc.Set("key2", "abcdef")
	if err != nil {
		t.Error(err)
		goto cleanup
	}
	_, err = rc.BitOp("and", "dest", "key1", "key2")
	if err != nil {
		t.Error(err)
	}
cleanup:
	rc.Delete("key1", "key2")
}

// TestBLPop reproduces the example from http://redis.io/commands/blpop.
func _TestBLPop(t *testing.T) {
	rc.Delete("list1", "list2")
	rc.RPush("list1", "a", "b", "c")
	k, v, err := rc.BLPop(0, "list1", "list2")
	if err != nil {
		t.Error(err)
		goto cleanup
	}
	if k != "list1" || v != "a" {
		t.Error(errMsg("k=" + k + " v=" + v))
	}
cleanup:
	rc.Delete("list1", "list2")
}

// TestBRPop reproduces the example from http://redis.io/commands/brpop.
func _TestBRPop(t *testing.T) {
	rc.Delete("list1", "list2")
	rc.RPush("list1", "a", "b", "c")
	k, v, err := rc.BRPop(0, "list1", "list2")
	if err != nil {
		t.Error(err)
		goto cleanup
	}
	if k != "list1" || v != "c" {
		t.Error(errMsg("k=" + k + " v=" + v))
	}
cleanup:
	rc.Delete("list1", "list2")
}

// TestBRPopTimeout is the same as TestBRPop, but expects a time out.
// TestBRPopTimeout also tests BLPop (because both share the same code).
func _TestBRPopTimeout(t *testing.T) {
	rc.Delete("list1", "list2")
	k, v, err := rc.BRPop(1, "list1", "list2")
	if err != ErrTimedOut {
		if err != nil {
			t.Error(errMsg(err))
		} else {
			t.Error(errMsg("k=" + k + " v=" + v))
		}
	}
	rc.Delete("list1", "list2")
}

// TestBRPopLPush takes last item of a list and inserts into another.
func _TestBRPopLPush(t *testing.T) {
	rc.Delete("list1", "list2")
	rc.RPush("list1", "a", "b", "c")
	v, err := rc.BRPopLPush("list1", "list2", 0)
	if err != nil {
		t.Error(err)
		goto cleanup
	}
	if v != "c" {
		t.Error(errMsg("v=" + v))
	}
cleanup:
	rc.Delete("list1", "list2")
}

// TestBRPopLPushTimeout is the same as TestBRPopLPush, but expects a time out.
func _TestBRPopLPushTimeout(t *testing.T) {
	rc.Delete("list1", "list2")
	v, err := rc.BRPopLPush("list1", "list2", 1)
	if err != ErrTimedOut {
		if err != nil {
			t.Error(errMsg(err))
		} else {
			t.Error(errMsg("v=" + v))
		}
	}
	rc.Delete("list1", "list2")
}

// TestClientListKill kills the first connection returned by CLIENT LIST.
func _TestClientListKill(t *testing.T) {
	clients, err := rc.ClientList()
	if err != nil {
		t.Error(errMsg(err))
		return
	}
	if len(clients) < 1 {
		t.Error("Unexpected response from CLIENT LIST")
		return
	}
	addr := strings.Split(clients[0], " ")
	err = rc.ClientKill(addr[0][5:]) // skip 'addr='
	if err != nil {
		t.Error(errMsg(err))
	}
	rc.ClientList() // send any cmd to enforce socket shutdown
}

// TestClientSetName name the current connection, and looks it up in the list.
func _TestClientSetName(t *testing.T) {
	err := rc.ClientSetName("bozo")
	if err != nil {
		t.Error(errMsg(err))
		return
	}
	clients, err := rc.ClientList()
	if err != nil {
		t.Error(errMsg(err))
		return
	}
	if len(clients) < 1 {
		t.Error("Unexpected response from CLIENT LIST ")
		return
	}
	found := false
	for _, info := range clients {
		if strings.Contains(info, " name=bozo ") {
			found = true
			break
		}
	}
	if !found {
		t.Error("Could not find client after SetName")
	}
}

// TestConfigGet tests the server port number.
func _TestConfigGet(t *testing.T) {
	items, err := rc.ConfigGet("*")
	if err != nil {
		t.Error(errMsg(err))
		return
	}
	if items["port"] != "6379" {
		t.Error(errMsg(items))
	}
}

// TestConfigSet sets redis dir to /tmp, and back to the default.
func TestConfigSet(t *testing.T) {
	items, err := rc.ConfigGet("dir")
	if err != nil {
		t.Error(errMsg(err))
		return
	}
	err = rc.ConfigSet("dir", "/tmp")
	if err != nil {
		t.Error(errMsg(err))
		return
	}
	err = rc.ConfigSet("dir", items["dir"])
	if err != nil {
		t.Error(errMsg(err))
		return
	}
}

// TestConfigResetStat resets redis statistics.
func TestConfigResetStat(t *testing.T) {
	err := rc.ConfigResetStat()
	if err != nil {
		t.Error(errMsg(err))
		return
	}
}

// TestSetAndGet sets a key, fetches it, and compare the results.
func _TestSetAndGet(t *testing.T) {
	k := randomString(1024)
	v := randomString(16 * 1024 * 1024)
	if err := rc.Set(k, v); err != nil {
		t.Error(err)
		return
	}
	val, err := rc.Get(k)
	if err != nil {
		t.Error(err)
		return
	}
	if val != v {
		t.Error(errMsg(val))
	}
	// try to clean up anyway
	rc.Delete(k)
}

// TestDelete creates 1024 keys and deletes them.
func _TestDelete(t *testing.T) {
	keys := make([]string, 1024)
	for n := 0; n < cap(keys); n++ {
		k := randomString(4) + string(n)
		v := randomString(32)
		if err := rc.Set(k, v); err != nil {
			t.Error(err)
			break // let it try to clean up
		} else {
			keys[n] = k
		}
	}
	deleted, err := rc.Delete(keys...)
	if err != nil {
		t.Error(err)
		return
	}
	if deleted != cap(keys) {
		t.Error(errMsg(deleted))
		return
	}
}
