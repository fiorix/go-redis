// Copyright 2013 go-redis authors
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package redis

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TODO: sort tests by dependency (set first, etc)

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
func TestAppend(t *testing.T) {
	defer func() { rc.Del("foobar") }()
	if _, err := rc.Append("foobar", "Hello"); err != nil {
		t.Error(err)
		return
	}
	if n, err := rc.Append("foobar", " World"); err != nil {
		t.Error(err)
	} else if n != 11 {
		t.Error(errUnexpected(n))
	}
}

// TestBgRewriteAOF starts an Append Only File rewrite process.
func __TestBgRewriteAOF(t *testing.T) {
	if status, err := rc.BgRewriteAOF(); err != nil {
		t.Error(err)
	} else if status != "Background append only file rewriting started" {
		t.Error(errUnexpected(status))
	}
}

// TestBgSave saves the DB in background.
func __TestBgSave(t *testing.T) {
	if status, err := rc.BgSave(); err != nil {
		t.Error(err)
	} else if status != "Background saving started" {
		t.Error(errUnexpected(status))
	}
}

// TestBitCount reproduces the example from http://redis.io/commands/bitcount.
func TestBitCount(t *testing.T) {
	defer func() { rc.Del("mykey") }()
	if err := rc.Set("mykey", "foobar"); err != nil {
		t.Error(err)
		return
	}
	if n, err := rc.BitCount("mykey", -1, -1); err != nil {
		t.Error(err)
	} else if n != 26 {
		t.Error(errUnexpected(n))
	}
}

// TestBitOp reproduces the example from http://redis.io/commands/bitop.
func TestBitOp(t *testing.T) {
	defer func() { rc.Del("key1", "key2") }()
	if err := rc.Set("key1", "foobar"); err != nil {
		t.Error(err)
		return
	}
	if err := rc.Set("key2", "abcdef"); err != nil {
		t.Error(err)
		return
	}
	if _, err := rc.BitOp("and", "dest", "key1", "key2"); err != nil {
		t.Error(err)
	}
}

// TestRPush and LIndex
func TestRPush(t *testing.T) {
	rc.Del("list1")
	rc.RPush("list1", "a", "b", "c")
	if v, err := rc.LIndex("list1", 1); err != nil {
		t.Error(err)
	} else if v != "b" {
		t.Error(errUnexpected("v=" + v))
	}
	rc.Del("list1")
}

// Test RPop
func TestRPop(t *testing.T) {
	rc.Del("list1")
	rc.RPush("list1", "a", "b", "c")
	if v, err := rc.RPop("list1"); err != nil {
		t.Error(err)
	} else if v != "c" {
		t.Error(errUnexpected("v=" + v))
	}
	rc.Del("list1")
}

// Test LPop
func TestLPop(t *testing.T) {
	rc.Del("list1")
	rc.RPush("list1", "a", "b", "c")
	if v, err := rc.LPop("list1"); err != nil {
		t.Error(err)
	} else if v != "a" {
		t.Error(errUnexpected("v=" + v))
	}
	rc.Del("list1")
}

func TestLLen(t *testing.T) {
	rc.Del("list1")
	rc.RPush("list1", "a", "b", "c")
	if v, err := rc.LLen("list1"); err != nil {
		t.Error(err)
	} else if v != 3 {
		t.Error(errUnexpected("v=" + string(v)))
	}
	rc.Del("list1")
}

func TestLTrim(t *testing.T) {
	rc.Del("list1")
	rc.RPush("list1", "a", "b", "c", "d")

	if err := rc.LTrim("list1", 2, 3); err != nil {
		t.Error(err)
	}

	if v, err := rc.LLen("list1"); err != nil {
		t.Error(err)
	} else if v != 2 {
		t.Error(errUnexpected("len list1 =" + string(v)))
	}

	rc.Del("list1")
}

func TestLRange(t *testing.T) {
	rc.Del("list1")
	rc.RPush("list1", "a", "b", "c", "d")

	if v, err := rc.LRange("list1", 0, 1); err != nil {
		t.Error(err)
	} else if v[0] != "a" {
		t.Error(errUnexpected("LRange list1"))
	}

	rc.Del("list1")
}

// TestBLPop reproduces the example from http://redis.io/commands/blpop.
func TestBLPop(t *testing.T) {
	rc.Del("list1", "list2")
	rc.RPush("list1", "a", "b", "c")
	if k, v, err := rc.BLPop(0, "list1", "list2"); err != nil {
		t.Error(err)
	} else if k != "list1" || v != "a" {
		t.Error(errUnexpected("k=" + k + " v=" + v))
	}
	rc.Del("list1", "list2")
}

// TestBRPop reproduces the example from http://redis.io/commands/brpop.
func TestBRPop(t *testing.T) {
	rc.Del("list1", "list2")
	rc.RPush("list1", "a", "b", "c")
	if k, v, err := rc.BRPop(0, "list1", "list2"); err != nil {
		t.Error(err)
	} else if k != "list1" || v != "c" {
		t.Error(errUnexpected("k=" + k + " v=" + v))
	}
	rc.Del("list1", "list2")
}

// TestBRPopTimeout is the same as TestBRPop, but expects a time out.
// TestBRPopTimeout also tests BLPop (because both share the same code).
func TestBRPopTimeout(t *testing.T) {
	rc.Del("list1", "list2")
	if k, v, err := rc.BRPop(1, "list1", "list2"); err != ErrTimedOut {
		if err != nil {
			t.Error(err)
		} else {
			t.Error(errUnexpected("k=" + k + " v=" + v))
		}
	}
	rc.Del("list1", "list2")
}

// TestBRPopLPush takes last item of a list and inserts into another.
func TestBRPopLPush(t *testing.T) {
	rc.Del("list1", "list2")
	rc.RPush("list1", "a", "b", "c")
	if v, err := rc.BRPopLPush("list1", "list2", 0); err != nil {
		t.Error(err)
	} else if v != "c" {
		t.Error(errUnexpected("v=" + v))
	}
	rc.Del("list1", "list2")
}

// TestBRPopLPushTimeout is the same as TestBRPopLPush, but expects a time out.
func TestBRPopLPushTimeout(t *testing.T) {
	rc.Del("list1", "list2")
	if v, err := rc.BRPopLPush("list1", "list2", 1); err != ErrTimedOut {
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
	var addr []string
	if clients, err := rc.ClientList(); err != nil {
		t.Error(err)
		return
	} else if len(clients) < 1 {
		t.Error(errUnexpected(clients))
		return
	} else {
		addr = strings.Split(clients[0], " ")
	}
	if err := rc.ClientKill(addr[0][5:]); err != nil {
		t.Error(err)
	}
	rc.ClientList() // send any cmd to enforce socket shutdown
}

// TestClientSetName name the current connection, and looks it up in the list.
func TestClientSetName(t *testing.T) {
	if err := rc.ClientSetName("bozo"); err != nil {
		t.Error(err)
		return
	}
	if clients, err := rc.ClientList(); err != nil {
		t.Error(err)
		return
	} else if len(clients) < 1 {
		t.Error(errUnexpected(clients))
		return
	} else {
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
}

// TestConfigGet tests the server port number.
func TestConfigGet(t *testing.T) {
	if items, err := rc.ConfigGet("*"); err != nil {
		t.Error(err)
	} else if _, ok := items["dbfilename"]; !ok {
		t.Error(errUnexpected(items))
	}
}

// TestConfigSet sets redis dir to /tmp, and back to the default.
func TestConfigSet(t *testing.T) {
	items, err := rc.ConfigGet("dir")
	if err != nil {
		t.Error(err)
		return
	}
	if err = rc.ConfigSet("dir", "/tmp"); err != nil {
		t.Error(err)
		return
	}
	if err := rc.ConfigSet("dir", items["dir"]); err != nil {
		t.Error(err)
	}
}

// TestConfigResetStat resets redis statistics.
func TestConfigResetStat(t *testing.T) {
	if err := rc.ConfigResetStat(); err != nil {
		t.Error(err)
	}
}

// TestDBSize checks the current database size, adds a key, and checks again.
func TestDBSize(t *testing.T) {
	size, err := rc.DBSize()
	if err != nil {
		t.Error(errUnexpected(err))
		return
	}
	rc.Set("test-db-size", "zzz")
	if new_size, err := rc.DBSize(); err != nil {
		t.Error(errUnexpected(err))
	} else if new_size != size+1 {
		t.Error(errUnexpected(new_size))
	}
	rc.Del("test-db-size")
}

// TestDebugSegfault crashes redis and breaks everything else.
func __TestDebugSegfault(t *testing.T) {
	if err := rc.DebugSegfault(); err != nil {
		t.Error(err)
	}
}

// TestDecr reproduces the example from http://redis.io/commands/decr.
func TestDecr(t *testing.T) {
	rc.Del("mykey")
	rc.Set("mykey", "10")
	if n, err := rc.Decr("mykey"); err != nil {
		t.Error(errUnexpected(err))
	} else if n != 9 {
		t.Error(errUnexpected(n))
	}
	rc.Del("mykey")
}

// TestDecrBy reproduces the example from http://redis.io/commands/decrby.
func TestDecrBy(t *testing.T) {
	rc.Del("mykey")
	rc.Set("mykey", "10")
	if n, err := rc.DecrBy("mykey", 5); err != nil {
		t.Error(errUnexpected(err))
	} else if n != 5 {
		t.Error(errUnexpected(n))
	}
	rc.Del("mykey")
}

// TestIncr reproduces the example from http://redis.io/commands/incr.
func TestIncr(t *testing.T) {
	rc.Del("mykey")
	rc.Set("mykey", "0")
	if n, err := rc.Incr("mykey"); err != nil {
		t.Error(errUnexpected(err))
	} else if n != 1 {
		t.Error(errUnexpected(n))
	}
	rc.Del("mykey")
}

// TestIncrBy reproduces the example from http://redis.io/commands/incrby.
func TestIncrBy(t *testing.T) {
	rc.Del("mykey")
	rc.Set("mykey", "0")
	if n, err := rc.IncrBy("mykey", 5); err != nil {
		t.Error(errUnexpected(err))
	} else if n != 5 {
		t.Error(errUnexpected(n))
	}
	rc.Del("mykey")
}

// TestDel creates 1024 keys and deletes them.
func TestDel(t *testing.T) {
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
	if deleted, err := rc.Del(keys...); err != nil {
		t.Error(err)
	} else if deleted != cap(keys) {
		t.Error(errUnexpected(deleted))
	}
}

// TODO: TestDiscard

// TestDump reproduces the example from http://redis.io/commands/dump.
func TestDump(t *testing.T) {
	rc.Set("mykey", "10")
	if v, err := rc.Dump("mykey"); err != nil {
		t.Error(err)
	} else if v != "\u0000\xC0\n\u0006\u0000\xF8r?\xC5\xFB\xFB_(" {
		t.Error(errUnexpected(v))
	}
	rc.Del("mykey")
}

// TestDump reproduces the example from http://redis.io/commands/echo.
func TestEcho(t *testing.T) {
	m := "Hello World!"
	if v, err := rc.Echo(m); err != nil {
		t.Error(err)
	} else if v != m {
		t.Error(errUnexpected(v))
	}
}

// TestEval tests server side Lua script.
// TODO: fix the response.
func TestEval(t *testing.T) {
	if _, err := rc.Eval(
		"return {1,{2,3,'foo'},KEYS[1],KEYS[2],ARGV[1],ARGV[2]}",
		2, // numkeys
		[]string{"key1", "key2"},    // keys
		[]string{"first", "second"}, // args
	); err != nil {
		t.Error(err)
	}
	//fmt.Printf("v=%#v\n", v)
}

// TestEvalSha tests server side Lua script.
// TestEvalSha preloads the script with ScriptLoad.
// TODO: fix the response.
func TestEvalSha(t *testing.T) {
	sha1, err := rc.ScriptLoad("return {1,{2,3,'foo'},KEYS[1],KEYS[2],ARGV[1],ARGV[2]}")
	if err != nil {
		t.Error(err)
		return
	}
	if _, err = rc.EvalSha(
		sha1, // pre-loaded script
		2,    // numkeys
		[]string{"key1", "key2"},    // keys
		[]string{"first", "second"}, // args
	); err != nil {
		t.Error(err)
	}
	//fmt.Printf("v=%#v\n", v)
}

// TODO: TestExec

// TestExists reproduces the example from http://redis.io/commands/exists.
func TestExists(t *testing.T) {
	rc.Del("key1", "key2")
	rc.Set("key1", "Hello")
	if ok, err := rc.Exists("key1"); err != nil {
		t.Error(err)
		return
	} else if !ok {
		t.Error(errUnexpected(ok))
		return
	}
	if ok, err := rc.Exists("key2"); err != nil {
		t.Error(err)
		return
	} else if ok {
		t.Error(errUnexpected(ok))
		return
	}
}

// TestExpire reproduces the example from http://redis.io/commands/expire.
// TestExpire also tests the TTL command.
func TestExpire(t *testing.T) {
	defer func() { rc.Del("mykey") }()
	rc.Set("mykey", "hello")
	if ok, err := rc.Expire("mykey", 10); err != nil {
		t.Error(err)
		return
	} else if !ok {
		t.Error(errUnexpected(ok))
		return
	}
	if ttl, err := rc.TTL("mykey"); err != nil {
		t.Error(err)
		return
	} else if ttl != 10 {
		t.Error(errUnexpected(ttl))
		return
	}
	rc.Set("mykey", "Hello World")
	if ttl, err := rc.TTL("mykey"); err != nil {
		t.Error(err)
	} else if ttl != -1 {
		t.Error(errUnexpected(ttl))
	}
}

// TestExpireAt reproduces the example from http://redis.io/commands/expire.
func TestExpireAt(t *testing.T) {
	defer func() { rc.Del("mykey") }()
	rc.Set("mykey", "hello")
	if ok, err := rc.Exists("mykey"); err != nil {
		t.Error(err)
		return
	} else if !ok {
		t.Error(errUnexpected(ok))
		return
	}
	if ok, err := rc.ExpireAt("mykey", 1293840000); err != nil {
		t.Error(err)
		return
	} else if !ok {
		t.Error(errUnexpected(ok))
		return
	}
	if ok, err := rc.Exists("mykey"); err != nil {
		t.Error(err)
	} else if ok {
		t.Error(errUnexpected(ok))
	}
}

// FlushAll and FlushDB are not required because they never fail.

// TestGet reproduces the example from http://redis.io/commands/get
func TestGet(t *testing.T) {
	rc.Del("nonexisting")
	if v, err := rc.Get("nonexisting"); err != nil {
		t.Error(err)
		return
	} else if v != "" {
		t.Error(errUnexpected(v))
		return
	}
	rc.Set("mykey", "Hello")
	if v, err := rc.Get("mykey"); err != nil {
		t.Error(err)
	} else if v == "" {
		t.Error(errUnexpected(v))
	}
	rc.Del("mykey")
}

// TestGetBit reproduces the example from http://redis.io/commands/getbit.
// TestGetBit also tests SetBit.
func TestGetBit(t *testing.T) {
	defer func() { rc.Del("mykey") }()
	if _, err := rc.SetBit("mykey", 7, 1); err != nil {
		t.Error(err)
		return
	}
	if v, err := rc.GetBit("mykey", 0); err != nil {
		t.Error(err)
		return
	} else if v != 0 {
		t.Error(errUnexpected(v))
		return
	}
	if v, err := rc.GetBit("mykey", 7); err != nil {
		t.Error(err)
	} else if v != 1 {
		t.Error(errUnexpected(v))
	}
}

// TestGetRange reproduces the example from http://redis.io/commands/getrange.
func TestGetRange(t *testing.T) {
	defer func() { rc.Del("mykey") }()
	rc.Set("mykey", "This is a string")
	if v, err := rc.GetRange("mykey", 0, 3); err != nil {
		t.Error(err)
		return
	} else if v != "This" {
		t.Error(errUnexpected(v))
		return
	}
	if v, err := rc.GetRange("mykey", -3, -1); err != nil {
		t.Error(err)
		return
	} else if v != "ing" {
		t.Error(errUnexpected(v))
		return
	}
	if v, err := rc.GetRange("mykey", 0, -1); err != nil {
		t.Error(err)
		return
	} else if v != "This is a string" {
		t.Error(errUnexpected(v))
		return
	}
	if v, err := rc.GetRange("mykey", 10, 100); err != nil {
		t.Error(err)
	} else if v != "string" {
		t.Error(errUnexpected(v))
	}
}

// TestGetSet reproduces the example from http://redis.io/commands/getset.
func TestGetSet(t *testing.T) {
	rc.Del("mycounter")
	rc.Incr("mycounter")
	if v, err := rc.GetSet("mycounter", "0"); err != nil {
		t.Error(err)
		return
	} else if v != "1" {
		t.Error(errUnexpected(v))
		return
	}
	if v, err := rc.Get("mycounter"); err != nil {
		t.Error(err)
	} else if v != "0" {
		t.Error(errUnexpected(v))
	}
	rc.Del("mycounter")
}

// TestMGet reproduces the example from http://redis.io/commands/mget.
func TestMGet(t *testing.T) {
	rc.Set("key1", "Hello")
	rc.Set("key2", "World")
	if items, err := rc.MGet("key1", "key2"); err != nil {
		t.Error(err)
	} else if items[0] != "Hello" || items[1] != "World" {
		t.Error(errUnexpected(items))
	}
	rc.Del("key1", "key2")
}

// TestMSet reproduces the example from http://redis.io/commands/mset.
func TestMSet(t *testing.T) {
	rc.Del("key1", "key2")
	if err := rc.MSet(map[string]string{
		"key1": "Hello", "key2": "World",
	}); err != nil {
		t.Error(err)
		return
	}
	v1, _ := rc.Get("key1")
	v2, _ := rc.Get("key2")
	if v1 != "Hello" || v2 != "World" {
		t.Error(errUnexpected(v1 + ", " + v2))
	}
	rc.Del("key1", "key2")
}

// TestKeys reproduces the example from http://redis.io/commands/keys
func TestKeys(t *testing.T) {
	rc.MSet(map[string]string{
		"one": "1", "two": "2", "three": "3", "four": "4",
	})
	defer func() { rc.Del("one", "two", "three", "four") }()
	keys, err := rc.Keys("*o*")
	if err != nil {
		t.Error(err)
		return
	}
	c := 0
	for _, k := range keys {
		switch k {
		case "one", "two", "four":
			c++
		}
	}
	if c != 3 {
		t.Error(errUnexpected(keys))
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
	if val, err := rc.Get(k); err != nil {
		t.Error(err)
		return
	} else if val != v {
		t.Error(errUnexpected(val))
	}
	// try to clean up anyway
	rc.Del(k)
}

// TestHIncrBy
func TestHIncrBy(t *testing.T) {
	rc.Del("mykey")
	if n, err := rc.HIncrBy("mykey", "beavis", 5); err != nil {
		t.Error(errUnexpected(err))
	} else if n != 5 {
		t.Error(errUnexpected(n))
	}
	rc.Del("mykey")
}

// TestHGet
func TestHGet(t *testing.T) {
	rc.Del("mykey")
	if _, err := rc.HIncrBy("mykey", "beavis", 5); err != nil {
		t.Error(errUnexpected(err))
	}
	if n, err := rc.HGet("mykey", "beavis"); err != nil {
		t.Error(errUnexpected(err))
	} else if n != "5" {
		t.Error(errUnexpected(n))
	}
	rc.Del("mykey")
}

// TestZIncrBy
func TestZIncrBy(t *testing.T) {
	rc.Del("mykey")
	if n, err := rc.ZIncrBy("mykey", 5, "beavis"); err != nil {
		t.Error(errUnexpected(err))
	} else if n != "5" {
		t.Error(errUnexpected(n))
	}
	rc.Del("mykey")
}

// TestZScore
func TestZScore(t *testing.T) {
	rc.Del("mykey")
	if _, err := rc.ZIncrBy("mykey", 5, "beavis"); err != nil {
		t.Error(errUnexpected(err))
	}
	if n, err := rc.ZScore("mykey", "beavis"); err != nil {
		t.Error(errUnexpected(err))
	} else if n != "5" {
		t.Error(errUnexpected(n))
	}
	rc.Del("mykey")
}

// Test ZAdd
func TestZAdd(t *testing.T) {
	rc.Del("myzset")
	if _, err := rc.ZAdd("myzset", 1, "beavis"); err != nil {
		t.Error(errUnexpected(err))
	}
	if n, err := rc.ZAdd("myzset", 2, "butthead", 3, "professor_buzzcut"); err != nil {
		t.Error(errUnexpected(err))
	} else if n != 2 {
		t.Error(errUnexpected(n))
	}
	rc.Del("myzset")
}

// Test ZRange
func TestZRange(t *testing.T) {
	rc.Del("myzset")
	if _, err := rc.ZAdd("myzset", 1, "beavis", 2, "butthead", 3, "professor_buzzcut"); err != nil {
		t.Error(errUnexpected(err))
	}

	if n, err := rc.ZRange("myzset", 0, 1, false); err != nil {
		t.Error(errUnexpected(err))
	} else if len(n) != 2 {
		t.Error(errUnexpected(n))
	}

	if n, err := rc.ZRange("myzset", 0, 1, true); err != nil {
		t.Error(errUnexpected(err))
	} else if len(n) != 4 {
		t.Error(errUnexpected(n))
	} else if n[0] != "beavis" && n[2] != "butthead" {
		t.Error(errUnexpected(n))
	}

	rc.Del("myzset")
}

// Test ZCard
func TestZCard(t *testing.T) {
	rc.Del("myzset")
	if _, err := rc.ZAdd("myzset", 1, "beavis", 2, "butthead", 3, "professor_buzzcut"); err != nil {
		t.Error(errUnexpected(err))
	}

	if n, err := rc.ZCard("myzset"); err != nil {
		t.Error(errUnexpected(err))
	} else if n != 3 {
		t.Error(errUnexpected(n))
	}
	rc.Del("myzset")
}

// Test ZCount
func TestZCount(t *testing.T) {
	rc.Del("myzset")
	if _, err := rc.ZAdd("myzset", 1, "beavis", 2, "butthead", 3, "professor_buzzcut"); err != nil {
		t.Error(errUnexpected(err))
	}

	if n, err := rc.ZCount("myzset", 0, 2); err != nil {
		t.Error(errUnexpected(err))
	} else if n != 2 {
		t.Error(errUnexpected(n))
	}
	rc.Del("myzset")
}

// Benchmark plain Set
func BenchmarkSet(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if err := rc.Set("foo", "bar"); err != nil {
			b.Error(err)
			return
		}
	}
}

// Benchmark plain Get
func BenchmarkGet(b *testing.B) {
	rc.Set("foo", "bar")
	for i := 0; i < b.N; i++ {
		if v, err := rc.Get("foo"); err != nil {
			b.Error(err)
			break
		} else if v != "bar" {
			b.Error(errUnexpected(v))
			break
		}
	}
	rc.Del("foo")
}

// Test/Benchmark INCRBY
func BenchmarkIncrBy(b *testing.B) {
	if err := rc.Set("foo", "0"); err != nil {
		b.Error(err)
		return
	}
	defer func() { rc.Del("foo") }()
	for i := 0; i < b.N; i++ {
		if _, err := rc.IncrBy("foo", 1); err != nil {
			b.Error(err)
			return
		}
	}
	if v, err := rc.Get("foo"); err != nil {
		b.Error(err)
	} else if v != strconv.Itoa(b.N) {
		b.Error("wrong incr result")
	}
}

// Benchmark DECR
func BenchmarkDecrBy(b *testing.B) {
	if err := rc.Set("foo", strconv.Itoa(b.N)); err != nil {
		b.Error(err)
		return
	}
	defer func() { rc.Del("foo") }()
	for i := 0; i < b.N; i++ {
		if _, err := rc.DecrBy("foo", 1); err != nil {
			b.Error(err)
			return
		}
	}
	if v, err := rc.Get("foo"); err != nil {
		b.Error(err)
	} else if v != "0" {
		b.Error("wrong decr result")
	}
}
