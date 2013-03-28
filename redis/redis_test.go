// Copyright 2013 Alexandre Fiori
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package redis

import (
	"math/rand"
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
		t.Error("Invalid response from redis:", n)
	}
cleanup:
	rc.Delete([]string{"foobar"})
}

// TestBgRewriteAOF starts an Append Only File rewrite process.
func _TestBgRewriteAOF(t *testing.T) {
	status, err := rc.BgRewriteAOF()
	if err != nil {
		t.Error(err)
	}
	if status != "Background append only file rewriting started" {
		t.Error("Invalid response from redis:", status)
	}
}

// TestBgSave saves the DB in background.
func TestBgSave(t *testing.T) {
	status, err := rc.BgSave()
	if err != nil {
		t.Error(err)
	}
	if status != "Background saving started" {
		t.Error("Invalid response from redis:", status)
	}
}

// TestBitCount reproduces this example: http://redis.io/commands/bitcount
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
		t.Error("Invalid response from redis:", n)
	}
cleanup:
	rc.Delete([]string{"foobar"})
}

// TestBitOp reproduces this example: http://redis.io/commands/bitop
func TestBitOp(t *testing.T) {
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
	rc.Delete([]string{"key1", "key2"})
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
		t.Error("Invalid response from redis:", val)
	}
	// try to clean up anyway
	rc.Delete([]string{k})
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
	deleted, err := rc.Delete(keys)
	if err != nil {
		t.Error(err)
		return
	}
	if deleted != cap(keys) {
		t.Error("Invalid response from redis:", deleted)
		return
	}
}
