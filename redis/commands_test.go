// Copyright 2013-2014 go-redis authors.  All rights reserved.
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

const errUnexpected = "Unexpected response from redis-server: %#v"

// Tests

func TestPublish(t *testing.T) {
  k := randomString(16)
  v := randomString(16)
  if err := rc.Publish(k, v); err != nil {
    t.Fatal(err)
  }
}

// TestAppend appends " World" to "Hello" and expects the lenght to be 11.
func TestAppend(t *testing.T) {
  defer rc.Del("foobar")
  if _, err := rc.Append("foobar", "Hello"); err != nil {
    t.Fatal(err)
  }
  if n, err := rc.Append("foobar", " World"); err != nil {
    t.Fatal(err)
  } else if n != 11 {
    t.Fatalf(errUnexpected, n)
  }
}

// TestBgRewriteAOF starts an Append Only File rewrite process.
func __TestBgRewriteAOF(t *testing.T) {
  if status, err := rc.BgRewriteAOF(); err != nil {
    t.Fatal(err)
  } else if status != "Background append only file rewriting started" {
    t.Fatalf(errUnexpected, status)
  }
}

// TestBgSave saves the DB in background.
func __TestBgSave(t *testing.T) {
  if status, err := rc.BgSave(); err != nil {
    t.Fatal(err)
  } else if status != "Background saving started" {
    t.Fatalf(errUnexpected, status)
  }
}

// TestBitCount reproduces the example from http://redis.io/commands/bitcount.
func TestBitCount(t *testing.T) {
  defer rc.Del("mykey")
  if err := rc.Set("mykey", "foobar"); err != nil {
    t.Fatal(err)
  }
  if n, err := rc.BitCount("mykey", -1, -1); err != nil {
    t.Fatal(err)
  } else if n != 26 {
    t.Fatalf(errUnexpected, n)
  }
}

// TestBitOp reproduces the example from http://redis.io/commands/bitop.
func TestBitOp(t *testing.T) {
  defer rc.Del("key1", "key2", "dest")
  if err := rc.Set("key1", "foobar"); err != nil {
    t.Fatal(err)
  }
  if err := rc.Set("key2", "abcdef"); err != nil {
    t.Fatal(err)
  }
  if _, err := rc.BitOp("and", "dest", "key1", "key2"); err != nil {
    t.Fatal(err)
  }
}

// TestRPush and LIndex
func TestRPush(t *testing.T) {
  rc.Del("list1")
  defer rc.Del("list1")
  rc.RPush("list1", "a", "b", "c")
  if v, err := rc.LIndex("list1", 1); err != nil {
    t.Fatal(err)
  } else if v != "b" {
    t.Fatalf(errUnexpected, "v="+v)
  }
}

// Test RPop
func TestRPop(t *testing.T) {
  rc.Del("list1")
  defer rc.Del("list1")
  rc.RPush("list1", "a", "b", "c")
  if v, err := rc.RPop("list1"); err != nil {
    t.Fatal(err)
  } else if v != "c" {
    t.Fatalf(errUnexpected, "v="+v)
  }
}

// Test LPop
func TestLPop(t *testing.T) {
  rc.Del("list1")
  defer rc.Del("list1")
  rc.RPush("list1", "a", "b", "c")
  if v, err := rc.LPop("list1"); err != nil {
    t.Fatal(err)
  } else if v != "a" {
    t.Fatalf(errUnexpected, "v="+v)
  }
}

func TestLLen(t *testing.T) {
  rc.Del("list1")
  defer rc.Del("list1")
  rc.RPush("list1", "a", "b", "c")
  if v, err := rc.LLen("list1"); err != nil {
    t.Fatal(err)
  } else if v != 3 {
    t.Fatalf(errUnexpected, "v="+string(v))
  }
}

func TestLTrim(t *testing.T) {
  rc.Del("list1")
  defer rc.Del("list1")
  rc.RPush("list1", "a", "b", "c", "d")

  if err := rc.LTrim("list1", 2, 3); err != nil {
    t.Fatal(err)
  }

  if v, err := rc.LLen("list1"); err != nil {
    t.Fatal(err)
  } else if v != 2 {
    t.Fatalf(errUnexpected, "len list1 ="+string(v))
  }
}

func TestLRange(t *testing.T) {
  rc.Del("list1")
  defer rc.Del("list1")
  rc.RPush("list1", "a", "b", "c", "d")

  if v, err := rc.LRange("list1", 0, 1); err != nil {
    t.Fatal(err)
  } else if v[0] != "a" {
    t.Fatalf(errUnexpected, "LRange list1")
  }
}

// TestBLPop reproduces the example from http://redis.io/commands/blpop.
func TestBLPop(t *testing.T) {
  rc.Del("list1", "list2")
  defer rc.Del("list1", "list2")
  rc.RPush("list1", "a", "b", "c")
  if k, v, err := rc.BLPop(0, "list1", "list2"); err != nil {
    t.Fatal(err)
  } else if k != "list1" || v != "a" {
    t.Fatalf(errUnexpected, "k="+k+" v="+v)
  }
}

// TestBRPop reproduces the example from http://redis.io/commands/brpop.
func TestBRPop(t *testing.T) {
  rc.Del("list1", "list2")
  defer rc.Del("list1", "list2")
  rc.RPush("list1", "a", "b", "c")
  if k, v, err := rc.BRPop(0, "list1", "list2"); err != nil {
    t.Fatal(err)
  } else if k != "list1" || v != "c" {
    t.Fatalf(errUnexpected, "k="+k+" v="+v)
  }
}

// TestBRPopTimeout is the same as TestBRPop, but expects a time out.
// TestBRPopTimeout also tests BLPop (because both share the same code).
func TestBRPopTimeout(t *testing.T) {
  rc.Del("list1", "list2")
  defer rc.Del("list1", "list2")
  if k, v, err := rc.BRPop(1, "list1", "list2"); err != ErrTimedOut {
    if err != nil {
      t.Fatal(err)
    } else {
      t.Fatalf(errUnexpected, "k="+k+" v="+v)
    }
  }
}

// TestBRPopTimeout2 is the same as TestBRPop, but expects a value.
func TestBRPopTimeout2(t *testing.T) {
  rc.Del("list1", "list2")
  defer rc.Del("list1", "list2")
  go func() {
    time.Sleep(100 * time.Millisecond)
    rc.LPush("list1", "a", "b", "c")
  }()
  if k, v, err := rc.BRPop(1, "list1", "list2"); err != nil {
    t.Fatal(err)
  } else if k != "list1" || v != "a" {
    t.Fatalf(errUnexpected, "k="+k+" v="+v)
  }
}

// TestBRPopLPush takes last item of a list and inserts into another.
func TestBRPopLPush(t *testing.T) {
  rc.Del("list1", "list2")
  defer rc.Del("list1", "list2")
  rc.RPush("list1", "a", "b", "c")
  if v, err := rc.BRPopLPush("list1", "list2", 0); err != nil {
    t.Fatal(err)
  } else if v != "c" {
    t.Fatalf(errUnexpected, "v="+v)
  }
}

// TestBRPopLPushTimeout is the same as TestBRPopLPush, but expects a time out.
func TestBRPopLPushTimeout(t *testing.T) {
  rc.Del("list1", "list2")
  defer rc.Del("list1", "list2")
  if v, err := rc.BRPopLPush("list1", "list2", 1); err != ErrTimedOut {
    if err != nil {
      t.Fatal(err)
    } else {
      t.Fatalf(errUnexpected, "v="+v)
    }
  }
}

// TestClientListKill kills the first connection returned by CLIENT LIST.
func TestClientListKill(t *testing.T) {
  var addr []string
  if clients, err := rc.ClientList(); err != nil {
    t.Fatal(err)
  } else if len(clients) < 1 {
    t.Fatalf(errUnexpected, clients)
  } else {
    addr = strings.Split(clients[0], " ")
  }
  defer rc.ClientList() // send any cmd to enforce socket shutdown
  if err := rc.ClientKill(addr[0][5:]); err != nil {
    t.Fatal(err)
  }
}

// TestClientSetName name the current connection, and looks it up in the list.
func TestClientSetName(t *testing.T) {
  if err := rc.ClientSetName("bozo"); err != nil {
    t.Fatal(err)
  }
  if clients, err := rc.ClientList(); err != nil {
    t.Fatal(err)
  } else if len(clients) < 1 {
    t.Fatalf(errUnexpected, clients)
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
    t.Fatal(err)
  } else if _, ok := items["dbfilename"]; !ok {
    t.Fatalf(errUnexpected, items)
  }
}

// TestConfigSet sets redis dir to /tmp, and back to the default.
func TestConfigSet(t *testing.T) {
  items, err := rc.ConfigGet("dir")
  if err != nil {
    t.Fatal(err)
  }
  if err = rc.ConfigSet("dir", "/tmp"); err != nil {
    t.Fatal(err)
  }
  if err := rc.ConfigSet("dir", items["dir"]); err != nil {
    t.Fatal(err)
  }
}

// TestConfigResetStat resets redis statistics.
func TestConfigResetStat(t *testing.T) {
  if err := rc.ConfigResetStat(); err != nil {
    t.Fatal(err)
  }
}

// TestDBSize checks the current database size, adds a key, and checks again.
func TestDBSize(t *testing.T) {
  size, err := rc.DBSize()
  if err != nil {
    t.Fatalf(errUnexpected, err)
  }
  rc.Set("test-db-size", "zzz")
  defer rc.Del("test-db-size")
  if new_size, err := rc.DBSize(); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if new_size != size+1 {
    t.Fatalf(errUnexpected, new_size)
  }
}

// TestDebugSegfault crashes redis and breaks everything else.
func __TestDebugSegfault(t *testing.T) {
  if err := rc.DebugSegfault(); err != nil {
    t.Fatal(err)
  }
}

// TestDecr reproduces the example from http://redis.io/commands/decr.
func TestDecr(t *testing.T) {
  rc.Del("mykey")
  defer rc.Del("mykey")
  rc.Set("mykey", "10")
  if n, err := rc.Decr("mykey"); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if n != 9 {
    t.Fatalf(errUnexpected, n)
  }
}

// TestDecrBy reproduces the example from http://redis.io/commands/decrby.
func TestDecrBy(t *testing.T) {
  rc.Del("mykey")
  defer rc.Del("mykey")
  rc.Set("mykey", "10")
  if n, err := rc.DecrBy("mykey", 5); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if n != 5 {
    t.Fatalf(errUnexpected, n)
  }
}

// TestIncr reproduces the example from http://redis.io/commands/incr.
func TestIncr(t *testing.T) {
  rc.Del("mykey")
  defer rc.Del("mykey")
  rc.Set("mykey", "0")
  if n, err := rc.Incr("mykey"); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if n != 1 {
    t.Fatalf(errUnexpected, n)
  }
}

// TestIncrBy reproduces the example from http://redis.io/commands/incrby.
func TestIncrBy(t *testing.T) {
  rc.Del("mykey")
  defer rc.Del("mykey")
  rc.Set("mykey", "0")
  if n, err := rc.IncrBy("mykey", 5); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if n != 5 {
    t.Fatalf(errUnexpected, n)
  }
}

// TestDel creates 1024 keys and deletes them.
func TestDel(t *testing.T) {
  keys := make([]string, 1024)
  for n := 0; n < cap(keys); n++ {
    k := randomString(4) + string(n)
    v := randomString(32)
    if err := rc.Set(k, v); err != nil {
      t.Fatal(err)
    } else {
      keys[n] = k
    }
  }
  if deleted, err := rc.Del(keys...); err != nil {
    t.Fatal(err)
  } else if deleted != cap(keys) {
    t.Fatalf(errUnexpected, deleted)
  }
}

// TODO: TestDiscard

// TestDump reproduces the example from http://redis.io/commands/dump.
func TestDump(t *testing.T) {
  defer rc.Del("mykey")
  rc.Set("mykey", "10")
  if v, err := rc.Dump("mykey"); err != nil {
    t.Fatal(err)
  } else if v != "\u0000\xC0\n\u0006\u0000\xF8r?\xC5\xFB\xFB_(" {
    t.Fatalf(errUnexpected, v)
  }
}

// TestDump reproduces the example from http://redis.io/commands/echo.
func TestEcho(t *testing.T) {
  m := "Hello World!"
  if v, err := rc.Echo(m); err != nil {
    t.Fatal(err)
  } else if v != m {
    t.Fatalf(errUnexpected, v)
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
    t.Fatal(err)
  }
  //t.Log("v=%#v\n", v)
}

// TestEvalSha tests server side Lua script.
// TestEvalSha preloads the script with ScriptLoad.
// TODO: fix the response.
func TestEvalSha(t *testing.T) {
  sha1, err := rc.ScriptLoad("return {1,{2,3,'foo'},KEYS[1],KEYS[2],ARGV[1],ARGV[2]}")
  if err != nil {
    t.Fatal(err)
  }
  if _, err = rc.EvalSha(
    sha1, // pre-loaded script
    2,    // numkeys
    []string{"key1", "key2"},    // keys
    []string{"first", "second"}, // args
  ); err != nil {
    t.Fatal(err)
  }
  //t.Log("v=%#v\n", v)
}

// TODO: TestExec

// TestExists reproduces the example from http://redis.io/commands/exists.
func TestExists(t *testing.T) {
  rc.Del("key1", "key2")
  defer rc.Del("key1", "key2")
  rc.Set("key1", "Hello")
  if ok, err := rc.Exists("key1"); err != nil {
    t.Fatal(err)
  } else if !ok {
    t.Fatalf(errUnexpected, ok)
  }
  if ok, err := rc.Exists("key2"); err != nil {
    t.Fatal(err)
  } else if ok {
    t.Fatalf(errUnexpected, ok)
  }
}

// TestExpire reproduces the example from http://redis.io/commands/expire.
// TestExpire also tests the TTL command.
func TestExpire(t *testing.T) {
  defer rc.Del("mykey")
  rc.Set("mykey", "hello")
  if ok, err := rc.Expire("mykey", 10); err != nil {
    t.Fatal(err)
  } else if !ok {
    t.Fatalf(errUnexpected, ok)
  }
  if ttl, err := rc.TTL("mykey"); err != nil {
    t.Fatal(err)
  } else if ttl != 10 {
    t.Fatalf(errUnexpected, ttl)
  }
  rc.Set("mykey", "Hello World")
  if ttl, err := rc.TTL("mykey"); err != nil {
    t.Fatal(err)
  } else if ttl != -1 {
    t.Fatalf(errUnexpected, ttl)
  }
}

// TestExpireAt reproduces the example from http://redis.io/commands/expire.
func TestExpireAt(t *testing.T) {
  defer rc.Del("mykey")
  rc.Set("mykey", "hello")
  if ok, err := rc.Exists("mykey"); err != nil {
    t.Fatal(err)
  } else if !ok {
    t.Fatalf(errUnexpected, ok)
  }
  if ok, err := rc.ExpireAt("mykey", 1293840000); err != nil {
    t.Fatal(err)
  } else if !ok {
    t.Fatalf(errUnexpected, ok)
  }
  if ok, err := rc.Exists("mykey"); err != nil {
    t.Fatal(err)
  } else if ok {
    t.Fatal(errUnexpected, ok)
  }
}

// FlushAll and FlushDB are not required because they never fail.

// TestGet reproduces the example from http://redis.io/commands/get
func TestGet(t *testing.T) {
  rc.Del("nonexisting")
  if v, err := rc.Get("nonexisting"); err != nil {
    t.Fatal(err)
  } else if v != "" {
    t.Fatalf(errUnexpected, v)
  }
  rc.Set("mykey", "Hello")
  defer rc.Del("mykey")
  if v, err := rc.Get("mykey"); err != nil {
    t.Fatal(err)
  } else if v == "" {
    t.Fatalf(errUnexpected, v)
  }
}

// TestGetBit reproduces the example from http://redis.io/commands/getbit.
// TestGetBit also tests SetBit.
func TestGetBit(t *testing.T) {
  defer rc.Del("mykey")
  if _, err := rc.SetBit("mykey", 7, 1); err != nil {
    t.Fatal(err)
  }
  if v, err := rc.GetBit("mykey", 0); err != nil {
    t.Fatal(err)
  } else if v != 0 {
    t.Fatalf(errUnexpected, v)
  }
  if v, err := rc.GetBit("mykey", 7); err != nil {
    t.Fatal(err)
  } else if v != 1 {
    t.Fatalf(errUnexpected, v)
  }
}

// TestGetRange reproduces the example from http://redis.io/commands/getrange.
func TestGetRange(t *testing.T) {
  defer rc.Del("mykey")
  rc.Set("mykey", "This is a string")
  if v, err := rc.GetRange("mykey", 0, 3); err != nil {
    t.Fatal(err)
  } else if v != "This" {
    t.Fatalf(errUnexpected, v)
  }
  if v, err := rc.GetRange("mykey", -3, -1); err != nil {
    t.Fatal(err)
  } else if v != "ing" {
    t.Fatalf(errUnexpected, v)
  }
  if v, err := rc.GetRange("mykey", 0, -1); err != nil {
    t.Fatal(err)
  } else if v != "This is a string" {
    t.Fatalf(errUnexpected, v)
  }
  if v, err := rc.GetRange("mykey", 10, 100); err != nil {
    t.Fatal(err)
  } else if v != "string" {
    t.Fatal(errUnexpected, v)
  }
}

// TestGetSet reproduces the example from http://redis.io/commands/getset.
func TestGetSet(t *testing.T) {
  rc.Del("mycounter")
  defer rc.Del("mycounter")
  rc.Incr("mycounter")
  if v, err := rc.GetSet("mycounter", "0"); err != nil {
    t.Fatal(err)
  } else if v != "1" {
    t.Fatalf(errUnexpected, v)
  }
  if v, err := rc.Get("mycounter"); err != nil {
    t.Fatal(err)
  } else if v != "0" {
    t.Fatalf(errUnexpected, v)
  }
}

// TestMGet reproduces the example from http://redis.io/commands/mget.
func TestMGet(t *testing.T) {
  defer rc.Del("key1", "key2")
  rc.Set("key1", "Hello")
  rc.Set("key2", "World")
  if items, err := rc.MGet("key1", "key2"); err != nil {
    t.Fatal(err)
  } else if items[0] != "Hello" || items[1] != "World" {
    t.Fatalf(errUnexpected, items)
  }
}

// TestMSet reproduces the example from http://redis.io/commands/mset.
func TestMSet(t *testing.T) {
  rc.Del("key1", "key2")
  defer rc.Del("key1", "key2")
  if err := rc.MSet(map[string]string{
    "key1": "Hello", "key2": "World",
  }); err != nil {
    t.Fatal(err)
  }
  v1, _ := rc.Get("key1")
  v2, _ := rc.Get("key2")
  if v1 != "Hello" || v2 != "World" {
    t.Fatalf(errUnexpected, v1+", "+v2)
  }
}

// TestKeys reproduces the example from http://redis.io/commands/keys
func TestKeys(t *testing.T) {
  rc.MSet(map[string]string{
    "one": "1", "two": "2", "three": "3", "four": "4",
  })
  defer rc.Del("one", "two", "three", "four")
  keys, err := rc.Keys("*o*")
  if err != nil {
    t.Fatal(err)
  }
  c := 0
  for _, k := range keys {
    switch k {
    case "one", "two", "four":
      c++
    }
  }
  if c != 3 {
    t.Fatalf(errUnexpected, keys)
  }
}

func TestSAdd(t *testing.T) {
  k := randomString(1024)
  defer rc.Del(k)

  //singles
  for i := 0; i < 10; i++ {
    v := randomString(32)
    _, err := rc.SAdd(k, v)
    if err != nil {
      t.Fatal(err)
    }
  }

  //multiple
  _, err := rc.SAdd(k, "setuno", "setdue")
  if err != nil {
    t.Fatal(err)
  }
}

func TestSMembers(t *testing.T) {
  k := randomString(1024)
  defer rc.Del(k)
  rc.SAdd(k, "setuno", "setdue")
  members, err := rc.SMembers(k)
  if err != nil {
    t.Fatal(err)
  }

  if len(members) != 2 {
    t.Fatalf(errUnexpected, len(members))
  }
}

// TestSetAndGet sets a key, fetches it, and compare the results.
func _TestSetAndGet(t *testing.T) {
  k := randomString(1024)
  v := randomString(16 * 1024 * 1024)
  defer rc.Del(k)
  if err := rc.Set(k, v); err != nil {
    t.Fatal(err)
  }
  if val, err := rc.Get(k); err != nil {
    t.Fatal(err)
  } else if val != v {
    t.Fatalf(errUnexpected, val)
  }
}

// TestSetEx sets a key to expire in 10s and checks the result.
func TestSetEx(t *testing.T) {
  k := randomString(16)
  defer rc.Del(k)
  if err := rc.SetEx(k, 10, "foobar"); err != nil {
    t.Fatal(err)
  }
}

func TestPing(t *testing.T) {
  if err := rc.Ping(); err != nil {
    t.Fatal(err)
  }
}

// TestHIncrBy
func TestHIncrBy(t *testing.T) {
  rc.Del("mykey")
  defer rc.Del("mykey")
  if n, err := rc.HIncrBy("mykey", "beavis", 5); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if n != 5 {
    t.Fatalf(errUnexpected, n)
  }
}

// TestHSet sets the a field in the hash and checks the result.
func TestHSet(t *testing.T) {
  rc.Del("mykey")
  defer rc.Del("mykey")
  if err := rc.HSet("mykey", "foo", "bar"); err != nil {
    t.Fatalf(errUnexpected, err)
  }
}

// TestHDel sets the a field in the hash and checks the result.
func TestHDel(t *testing.T) {
  rc.Del("mykey")
  defer rc.Del("mykey")
  rc.HSet("mykey", "foo", "bar")
  if err := rc.HDel("mykey", "foo"); err != nil {
    t.Fatalf(errUnexpected, err)
  }
}

// TestHGet sets a key and gets its value, checking the results.
func TestHGet(t *testing.T) {
  rc.Del("mykey")
  defer rc.Del("mykey")
  rc.HSet("mykey", "foo", "bar")
  if n, err := rc.HGet("mykey", "foo"); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if n != "bar" {
    t.Fatalf(errUnexpected, n)
  }
}

// TestHMSet sets multiple fields in the hash and checks the result.
func TestHMSet(t *testing.T) {
  rc.Del("mykey")
  defer rc.Del("mykey")
  if err := rc.HMSet("mykey", map[string]string{
    "foo":   "bar",
    "hello": "world",
  }); err != nil {
    t.Fatalf(errUnexpected, err)
  }
}

// TestHMGet sets fields in the hash, then get them and checks the results.
func TestHMGet(t *testing.T) {
  rc.Del("mykey")
  defer rc.Del("mykey")
  rc.HMSet("mykey", map[string]string{
    "foo":   "bar",
    "hello": "world",
  })
  if v, err := rc.HMGet("mykey", "foo", "hello"); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if len(v) != 2 || v[0] != "bar" || v[1] != "world" {
    t.Fatalf(errUnexpected, v)
  }
}

// TestHGetAll sets fields in the hash, get them all and checks the results.
func TestHGetAll(t *testing.T) {
  rc.Del("mykey")
  defer rc.Del("mykey")
  rc.HMSet("mykey", map[string]string{
    "foo":   "bar",
    "hello": "world",
  })
  if v, err := rc.HGetAll("mykey"); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if len(v) != 2 || v["foo"] != "bar" || v["hello"] != "world" {
    t.Fatalf(errUnexpected, v)
  }
}

// TestZIncrBy increments a field in a hash and checks the result.
func TestZIncrBy(t *testing.T) {
  rc.Del("mykey")
  defer rc.Del("mykey")
  if n, err := rc.ZIncrBy("mykey", 5, "beavis"); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if n != "5" {
    t.Fatalf(errUnexpected, n)
  }
}

// TestZScore
func TestZScore(t *testing.T) {
  rc.Del("mykey")
  defer rc.Del("mykey")
  if _, err := rc.ZIncrBy("mykey", 5, "beavis"); err != nil {
    t.Fatalf(errUnexpected, err)
  }
  if n, err := rc.ZScore("mykey", "beavis"); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if n != "5" {
    t.Fatalf(errUnexpected, n)
  }
}

// Test ZAdd
func TestZAdd(t *testing.T) {
  rc.Del("myzset")
  defer rc.Del("myzset")
  if _, err := rc.ZAdd("myzset", 1, "beavis"); err != nil {
    t.Fatalf(errUnexpected, err)
  }
  if n, err := rc.ZAdd("myzset", 2, "butthead", 3, "professor_buzzcut"); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if n != 2 {
    t.Fatalf(errUnexpected, n)
  }
}

// Test ZRem
func TestZRem(t *testing.T) {
  rc.Del("myzset")
  defer rc.Del("myzset")
  if _, err := rc.ZAdd("myzset", 1, "beavis", 2, "butthead", 3, "professor_buzzcut"); err != nil {
    t.Fatalf(errUnexpected, err)
  }
  if n, err := rc.ZRem("myzset", "beavis", "butthead", "professor_buzzcut"); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if n != 3 {
    t.Fatalf(errUnexpected, n)
  }
}

// Test ZRange
func TestZRange(t *testing.T) {
  rc.Del("myzset")
  defer rc.Del("myzset")
  if _, err := rc.ZAdd("myzset", 1, "beavis", 2, "butthead", 3, "professor_buzzcut"); err != nil {
    t.Fatalf(errUnexpected, err)
  }

  if n, err := rc.ZRange("myzset", 0, 1, false); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if len(n) != 2 {
    t.Fatalf(errUnexpected, n)
  }

  if n, err := rc.ZRange("myzset", 0, 1, true); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if len(n) != 4 {
    t.Fatalf(errUnexpected, n)
  } else if n[0] != "beavis" && n[2] != "butthead" {
    t.Fatalf(errUnexpected, n)
  }
}

// Test ZRevRange
func TestZRevRange(t *testing.T) {
  rc.Del("myzset")
  defer rc.Del("myzset")
  if _, err := rc.ZAdd("myzset", 1, "beavis", 2, "butthead", 3, "professor_buzzcut"); err != nil {
    t.Fatalf(errUnexpected, err)
  }

  if n, err := rc.ZRange("myzset", 0, 1, false); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if len(n) != 2 {
    t.Fatalf(errUnexpected, n)
  }

  if n, err := rc.ZRange("myzset", 0, 1, true); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if len(n) != 4 {
    t.Fatalf(errUnexpected, n)
  } else if n[0] != "professor_buzzcut" && n[2] != "butthead" {
    t.Fatalf(errUnexpected, n)
  }
}

// Test ZCard
func TestZCard(t *testing.T) {
  rc.Del("myzset")
  defer rc.Del("myzset")
  if _, err := rc.ZAdd("myzset", 1, "beavis", 2, "butthead", 3, "professor_buzzcut"); err != nil {
    t.Fatalf(errUnexpected, err)
  }

  if n, err := rc.ZCard("myzset"); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if n != 3 {
    t.Fatalf(errUnexpected, n)
  }
}

// Test ZCount
func TestZCount(t *testing.T) {
  rc.Del("myzset")
  defer rc.Del("myzset")
  if _, err := rc.ZAdd("myzset", 1, "beavis", 2, "butthead", 3, "professor_buzzcut"); err != nil {
    t.Fatalf(errUnexpected, err)
  }

  if n, err := rc.ZCount("myzset", 0, 2); err != nil {
    t.Fatalf(errUnexpected, err)
  } else if n != 2 {
    t.Fatalf(errUnexpected, n)
  }
}

// Benchmark plain Set
func BenchmarkSet(b *testing.B) {
  for i := 0; i < b.N; i++ {
    if err := rc.Set("foo", "bar"); err != nil {
      b.Fatal(err)
    }
  }
}

// Benchmark plain Get
func BenchmarkGet(b *testing.B) {
  defer rc.Del("foo")
  rc.Set("foo", "bar")
  for i := 0; i < b.N; i++ {
    if v, err := rc.Get("foo"); err != nil {
      b.Fatal(err)
    } else if v != "bar" {
      b.Fatalf(errUnexpected, v)
    }
  }
}

// Test/Benchmark INCRBY
func BenchmarkIncrBy(b *testing.B) {
  rc.Del("foo")
  if err := rc.Set("foo", "0"); err != nil {
    b.Fatal(err)
  }
  for i := 0; i < b.N; i++ {
    if _, err := rc.IncrBy("foo", 1); err != nil {
      b.Fatal(err)
    }
  }
  if v, err := rc.Get("foo"); err != nil {
    b.Fatal(err)
  } else if v != strconv.Itoa(b.N) {
    b.Fatalf(errUnexpected, v)
  }
}

// Benchmark DECR
func BenchmarkDecrBy(b *testing.B) {
  defer rc.Del("foo")
  if err := rc.Set("foo", strconv.Itoa(b.N)); err != nil {
    b.Fatal(err)
  }
  for i := 0; i < b.N; i++ {
    if _, err := rc.DecrBy("foo", 1); err != nil {
      b.Fatal(err)
    }
  }
  if v, err := rc.Get("foo"); err != nil {
    b.Fatal(err)
  } else if v != "0" {
    b.Fatalf(errUnexpected, v)
  }
}
