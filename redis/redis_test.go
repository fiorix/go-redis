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

func errUnexpected(msg interface{}) string {
	return fmt.Sprintf("Unexpected response from redis-server: %#v\n", msg)
}

// Tests

// TestAppend appends " World" to "Hello" and expects the lenght to be 11.
func _TestAppend(t *testing.T) {
	defer func() { rc.Del("foobar") }()
	n, err := rc.Append("foobar", "Hello")
	if err != nil {
		t.Error(err)
		return
	}
	n, err = rc.Append("foobar", " World")
	if err != nil {
		t.Error(err)
	} else if n != 11 {
		t.Error(errUnexpected(n))
	}
}

// TestBgRewriteAOF starts an Append Only File rewrite process.
func _TestBgRewriteAOF(t *testing.T) {
	status, err := rc.BgRewriteAOF()
	if err != nil {
		t.Error(err)
	} else if status != "Background append only file rewriting started" {
		t.Error(errUnexpected(status))
	}
}

// TestBgSave saves the DB in background.
func _TestBgSave(t *testing.T) {
	status, err := rc.BgSave()
	if err != nil {
		t.Error(err)
	} else if status != "Background saving started" {
		t.Error(errUnexpected(status))
	}
}

// TestBitCount reproduces the example from http://redis.io/commands/bitcount.
func _TestBitCount(t *testing.T) {
	defer func() { rc.Del("mykey") }()
	err := rc.Set("mykey", "foobar")
	if err != nil {
		t.Error(err)
		return
	}
	n, err := rc.BitCount("mykey", -1, -1)
	if err != nil {
		t.Error(err)
	} else if n != 26 {
		t.Error(errUnexpected(n))
	}
}

// TestBitOp reproduces the example from http://redis.io/commands/bitop.
func _TestBitOp(t *testing.T) {
	defer func() { rc.Del("key1", "key2") }()
	err := rc.Set("key1", "foobar")
	if err != nil {
		t.Error(err)
		return
	}
	err = rc.Set("key2", "abcdef")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = rc.BitOp("and", "dest", "key1", "key2")
	if err != nil {
		t.Error(err)
	}
}

// TestBLPop reproduces the example from http://redis.io/commands/blpop.
func _TestBLPop(t *testing.T) {
	rc.Del("list1", "list2")
	rc.RPush("list1", "a", "b", "c")
	k, v, err := rc.BLPop(0, "list1", "list2")
	if err != nil {
		t.Error(err)
	} else if k != "list1" || v != "a" {
		t.Error(errUnexpected("k=" + k + " v=" + v))
	}
	rc.Del("list1", "list2")
}

// TestBRPop reproduces the example from http://redis.io/commands/brpop.
func _TestBRPop(t *testing.T) {
	rc.Del("list1", "list2")
	rc.RPush("list1", "a", "b", "c")
	k, v, err := rc.BRPop(0, "list1", "list2")
	if err != nil {
		t.Error(err)
	} else if k != "list1" || v != "c" {
		t.Error(errUnexpected("k=" + k + " v=" + v))
	}
	rc.Del("list1", "list2")
}

// TestBRPopTimeout is the same as TestBRPop, but expects a time out.
// TestBRPopTimeout also tests BLPop (because both share the same code).
func _TestBRPopTimeout(t *testing.T) {
	rc.Del("list1", "list2")
	k, v, err := rc.BRPop(1, "list1", "list2")
	if err != ErrTimedOut {
		if err != nil {
			t.Error(err)
		} else {
			t.Error(errUnexpected("k=" + k + " v=" + v))
		}
	}
	rc.Del("list1", "list2")
}

// TestBRPopLPush takes last item of a list and inserts into another.
func _TestBRPopLPush(t *testing.T) {
	rc.Del("list1", "list2")
	rc.RPush("list1", "a", "b", "c")
	v, err := rc.BRPopLPush("list1", "list2", 0)
	if err != nil {
		t.Error(err)
	} else if v != "c" {
		t.Error(errUnexpected("v=" + v))
	}
	rc.Del("list1", "list2")
}

// TestBRPopLPushTimeout is the same as TestBRPopLPush, but expects a time out.
func _TestBRPopLPushTimeout(t *testing.T) {
	rc.Del("list1", "list2")
	v, err := rc.BRPopLPush("list1", "list2", 1)
	if err != ErrTimedOut {
		if err != nil {
			t.Error(err)
		} else {
			t.Error(errUnexpected("v=" + v))
		}
	}
	rc.Del("list1", "list2")
}

// TestClientListKill kills the first connection returned by CLIENT LIST.
func TestClientListKill(t *testing.T) {
	clients, err := rc.ClientList()
	if err != nil {
		t.Error(err)
		return
	}
	if len(clients) < 1 {
		t.Error(errUnexpected(clients))
		return
	}
	addr := strings.Split(clients[0], " ")
	err = rc.ClientKill(addr[0][5:]) // skip 'addr='
	if err != nil {
		t.Error(err)
	}
	rc.ClientList() // send any cmd to enforce socket shutdown
}

// TestClientSetName name the current connection, and looks it up in the list.
func _TestClientSetName(t *testing.T) {
	err := rc.ClientSetName("bozo")
	if err != nil {
		t.Error(err)
		return
	}
	clients, err := rc.ClientList()
	if err != nil {
		t.Error(err)
		return
	}
	if len(clients) < 1 {
		t.Error(errUnexpected(clients))
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
		t.Error(err)
	} else if items["port"] != "6379" {
		t.Error(errUnexpected(items))
	}
}

// TestConfigSet sets redis dir to /tmp, and back to the default.
func _TestConfigSet(t *testing.T) {
	items, err := rc.ConfigGet("dir")
	if err != nil {
		t.Error(err)
		return
	}
	err = rc.ConfigSet("dir", "/tmp")
	if err != nil {
		t.Error(err)
		return
	}
	err = rc.ConfigSet("dir", items["dir"])
	if err != nil {
		t.Error(err)
	}
}

// TestConfigResetStat resets redis statistics.
func _TestConfigResetStat(t *testing.T) {
	err := rc.ConfigResetStat()
	if err != nil {
		t.Error(err)
	}
}

// TestDBSize checks the current database size, adds a key, and checks again.
func _TestDBSize(t *testing.T) {
	size, err := rc.DBSize()
	if err != nil {
		t.Error(errUnexpected(err))
		return
	}
	defer func() { rc.Del("test-db-size") }()
	rc.Set("test-db-size", "zzz")
	new_size, err := rc.DBSize()
	if err != nil {
		t.Error(errUnexpected(err))
		return
	}
	if new_size != size+1 {
		t.Error(errUnexpected(new_size))
	}
}

// TestDebugSegfault crashes redis and breaks everything else.
func _TestDebugSegfault(t *testing.T) {
	err := rc.DebugSegfault()
	if err != nil {
		t.Error(err)
	}
}

// TestDecr reproduces the example from http://redis.io/commands/decr.
func _TestDecr(t *testing.T) {
	rc.Del("mykey")
	rc.Set("mykey", "10")
	n, err := rc.Decr("mykey")
	if err != nil {
		t.Error(errUnexpected(err))
	} else if n != 9 {
		t.Error(errUnexpected(n))
	}
	rc.Del("mykey")
}

// TestDecrBy reproduces the example from http://redis.io/commands/decrby.
func _TestDecrBy(t *testing.T) {
	rc.Del("mykey")
	rc.Set("mykey", "10")
	n, err := rc.DecrBy("mykey", 5)
	if err != nil {
		t.Error(errUnexpected(err))
	} else if n != 5 {
		t.Error(errUnexpected(n))
	}
	rc.Del("mykey")
}

// TestDel creates 1024 keys and deletes them.
func _TestDel(t *testing.T) {
	keys := make([]string, 1024)
	for n := 0; n < cap(keys); n++ {
		k := randomString(4) + string(n)
		v := randomString(32)
		if err := rc.Set(k, v); err != nil {
			t.Error(err)
			break
		} else {
			keys[n] = k
		}
	}
	deleted, err := rc.Del(keys...)
	if err != nil {
		t.Error(err)
	} else if deleted != cap(keys) {
		t.Error(errUnexpected(deleted))
	}
}

// TODO: TestDiscard

// TestDump reproduces the example from http://redis.io/commands/dump.
func _TestDump(t *testing.T) {
	rc.Set("mykey", "10")
	v, err := rc.Dump("mykey")
	if err != nil {
		t.Error(err)
	} else if v != "\u0000\xC0\n\u0006\u0000\xF8r?\xC5\xFB\xFB_(" {
		t.Error(errUnexpected(v))
	}
	rc.Del("mykey")
}

// TestDump reproduces the example from http://redis.io/commands/echo.
func _TestEcho(t *testing.T) {
	m := "Hello World!"
	v, err := rc.Echo(m)
	if err != nil {
		t.Error(err)
	} else if v != m {
		t.Error(errUnexpected(v))
	}
}

// TestEval tests server side Lua script.
// TODO: fix
func TestEval(t *testing.T) {
	_, err := rc.Eval(
		"return {1,{2,3,'foo'},KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
		2, // numkeys
		[]string{"key1", "key2"},    // keys
		[]string{"first", "second"}, // args
	)
	if err != nil {
		t.Error(err)
		return
	}
	//fmt.Printf("v=%#v\n", v)
}

// TestEvalSha tests server side Lua script.
// TestEvalSha preloads the script with ScriptLoad.
// TODO: fix
func TestEvalSha(t *testing.T) {
	sha1, err := rc.ScriptLoad("return {1,{2,3,'foo'},KEYS[1],KEYS[2],ARGV[1],ARGV[2]}")
	if err != nil {
		t.Error(err)
		return
	}
	_, err = rc.EvalSha(
		sha1, // pre-loaded script
		2,    // numkeys
		[]string{"key1", "key2"},    // keys
		[]string{"first", "second"}, // args
	)
	if err != nil {
		t.Error(err)
		return
	}
	//fmt.Printf("v=%#v\n", v)
}

// TODO: TestExec

// TestExists reproduces the example from http://redis.io/commands/exists.
func TestExists(t *testing.T) {
	rc.Del("key1", "key2")
	rc.Set("key1", "Hello")
	ok, err := rc.Exists("key1")
	if err != nil {
		t.Error(err)
		return
	}
	if !ok {
		t.Error(errUnexpected(ok))
		return
	}
	ok, err = rc.Exists("key2")
	if err != nil {
		t.Error(err)
		return
	}
	if ok {
		t.Error(errUnexpected(ok))
		return
	}
	rc.Del("key1", "key2")
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
		t.Error(errUnexpected(val))
	}
	// try to clean up anyway
	rc.Del(k)
}

// Benchmark plain Set
func BenchmarkSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		err := rc.Set("foo", "bar")
		if err != nil {
			b.Error(err)
			return
		}
	}
}

// Benchmark plain Get
func BenchmarkGet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		v, err := rc.Get("foo")
		if err != nil {
			b.Error(err)
			return
		}
		if v != "bar" {
			b.Error(errUnexpected(v))
			return
		}
	}
}
