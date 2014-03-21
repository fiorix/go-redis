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
//
// This is a modified version of gomemcache adapted to redis.
// Original code and license at https://github.com/bradfitz/gomemcache/

package redis

// WORK IN PROGRESS
//
// Redis commands
//
// Some commands take an integer timeout, in seconds. It's not a time.Duration
// because redis only supports seconds resolution for timeouts.
//
// Redis allows clients to block indefinitely by setting timeout to 0, but
// it does not work here. All functions below use the timeout not only to
// block the operation in redis, but also as a socket read timeout (+delta)
// to free up system resources.
//
// The default TCP read timeout is 100ms. If a timeout is required to
// be "indefinitely", then set it to something like 86400.
// 🍺

import (
	"errors"
	"strings"
	"time"
)

// http://redis.io/commands/append
func (c *Client) Append(key, value string) (int, error) {
	v, err := c.execWithKey(true, "append", key, value)
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/bgrewriteaof
// BgRewriteAOF is not fully supported on sharded connections.
func (c *Client) BgRewriteAOF() (string, error) {
	v, err := c.execOnFirst(false, "BGREWRITEAOF")
	if err != nil {
		return "", err
	}
	return iface2str(v)
}

// http://redis.io/commands/bgsave
// BgSave is not fully supported on sharded connections.
func (c *Client) BgSave() (string, error) {
	v, err := c.execOnFirst(false, "BGSAVE")
	if err != nil {
		return "", err
	}
	return iface2str(v)
}

// http://redis.io/commands/ping
// goes to first connection only
func (c *Client) Ping() error {
	v, err := c.execOnFirst(false, "PING")
	if err != nil {
		return err
	}
	s, err := iface2str(v)
	if err != nil {
		return err
	} else if s != "PONG" {
		return ErrServerError
	}
	return nil
}

// http://redis.io/commands/bitcount
// BitCount ignores start and end if start is a negative number.
func (c *Client) BitCount(key string, start, end int) (int, error) {
	var (
		v   interface{}
		err error
	)
	if start > -1 {
		v, err = c.execWithKey(true, "BITCOUNT", key, start, end)
	} else {
		v, err = c.execWithKey(true, "BITCOUNT", key)
	}
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/bitop
// BitOp is not fully supported on sharded connections.
func (c *Client) BitOp(operation, destkey, key string, keys ...string) (int, error) {
	a := append([]string{"BITOP", operation, destkey, key}, keys...)
	v, err := c.execOnFirst(true, vstr2iface(a)...)
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// blbrPop supports both BLPop and BRPop.
func (c *Client) blbrPop(cmd string, timeout int, keys ...string) (k, v string, err error) {
	t := c.Timeout
	// Extend the client's timeout for this operation only.
	// TODO: make sure it does not affect other concurrent calls.
	if t == 0 {
		c.Timeout = time.Duration(timeout) + DefaultTimeout
	} else {
		c.Timeout = time.Duration(timeout) + t
	}
	var r interface{}
	r, err = c.execWithKey(true, cmd, keys[0],
		append(vstr2iface(keys[1:]), timeout)...)
	c.Timeout = t
	if err != nil {
		return
	}
	if r == nil {
		err = ErrTimedOut
		return
	}
	switch r.(type) {
	case []interface{}:
		items := r.([]interface{})
		if len(items) != 2 {
			err = ErrServerError
			return
		}
		// TODO: test types
		k = items[0].(string)
		v = items[1].(string)
		return
	}
	err = ErrServerError
	return
}

// http://redis.io/commands/blpop
// BLPop is not fully supported on sharded connections.
func (c *Client) BLPop(timeout int, keys ...string) (k, v string, err error) {
	return c.blbrPop("BLPOP", timeout, keys...)
}

// http://redis.io/commands/brpop
// BRPop is not fully supported on sharded connections.
func (c *Client) BRPop(timeout int, keys ...string) (k, v string, err error) {
	return c.blbrPop("BRPOP", timeout, keys...)
}

// http://redis.io/commands/brpoplpush
// BRPopLPush is not fully supported on sharded connections.
func (c *Client) BRPopLPush(src, dst string, timeout int) (string, error) {
	t := c.Timeout
	// Extend the client's timeout for this operation only.
	// TODO: make sure it does not affect other concurrent calls.
	if t == 0 {
		c.Timeout = time.Duration(timeout)*time.Second + DefaultTimeout
	} else {
		c.Timeout = time.Duration(timeout)*time.Second + t
	}
	v, err := c.execWithKey(true, "BRPOPLPUSH", src, dst, timeout)
	c.Timeout = t
	if err != nil {
		return "", err
	} else if v == nil {
		return "", ErrTimedOut
	}
	return iface2str(v)
}

// http://redis.io/commands/client-kill
// ClientKill is not fully supported on sharded connections.
func (c *Client) ClientKill(kill_addr string) error {
	v, err := c.execOnFirst(false, "CLIENT KILL", kill_addr)
	if err != nil {
		return err
	}
	switch v.(type) {
	case string:
		return nil
	}
	return ErrServerError
}

// http://redis.io/commands/client-list
// ClientList is not fully supported on sharded connections.
func (c *Client) ClientList() ([]string, error) {
	v, err := c.execOnFirst(false, "CLIENT LIST")
	if err != nil {
		return nil, err
	}
	switch v.(type) {
	case string:
		return strings.Split(v.(string), "\n"), nil
	}
	return nil, ErrServerError
}

// http://redis.io/commands/client-setname
// ClientSetName is not fully supported on sharded connections, and is useless here.
// This driver creates connections on demand, thus naming them is pointless.
func (c *Client) ClientSetName(name string) error {
	v, err := c.execOnFirst(false, "CLIENT SETNAME", name)
	if err != nil {
		return err
	}
	switch v.(type) {
	case string:
		return nil
	}
	return ErrServerError
}

// http://redis.io/commands/config-get
// ConfigGet is not fully supported on sharded connections.
func (c *Client) ConfigGet(name string) (map[string]string, error) {
	v, err := c.execOnFirst(false, "CONFIG GET", name)
	if err != nil {
		return nil, err
	}
	return iface2strmap(v), nil
}

// http://redis.io/commands/config-set
// ConfigSet is not fully supported on sharded connections.
func (c *Client) ConfigSet(name, value string) error {
	v, err := c.execOnFirst(false, "CONFIG SET", name, value)
	if err != nil {
		return err
	}
	switch v.(type) {
	case string:
		return nil
	}
	return ErrServerError
}

// http://redis.io/commands/config-resetstat
// ConfigResetStat is not fully supported on sharded connections.
func (c *Client) ConfigResetStat() error {
	v, err := c.execOnFirst(false, "CONFIG RESETSTAT")
	if err != nil {
		return err
	}
	switch v.(type) {
	case string:
		return nil
	}
	return ErrServerError
}

// http://redis.io/commands/dbsize
// DBSize is not fully supported on sharded connections.
func (c *Client) DBSize() (int, error) {
	v, err := c.execOnFirst(false, "DBSIZE")
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/debug-segfault
// DebugSegfault is not fully supported on sharded connections.
func (c *Client) DebugSegfault() error {
	v, err := c.execOnFirst(false, "DEBUG SEGFAULT")
	if err != nil {
		return err
	}
	switch v.(type) {
	case string:
		return nil
	}
	return ErrServerError
}

// http://redis.io/commands/decr
func (c *Client) Decr(key string) (int, error) {
	v, err := c.execWithKey(true, "DECR", key)
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/decrby
func (c *Client) DecrBy(key string, decrement int) (int, error) {
	v, err := c.execWithKey(true, "DECRBY", key, decrement)
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/del
// Del issues a plain DEL command to redis if the client is connected to a
// single server. On sharding, it issues one DEL command per key, in the
// server selected for each given key.
func (c *Client) Del(keys ...string) (n int, err error) {
	if c.selector.Sharding() {
		n, err = c.delMulti(keys...)
	} else {
		n, err = c.delPlain(keys...)
	}
	return n, err
}

func (c *Client) delMulti(keys ...string) (int, error) {
	deleted := 0
	for _, key := range keys {
		count, err := c.delPlain(key)
		if err != nil {
			return 0, err
		}
		deleted += count
	}
	return deleted, nil
}

func (c *Client) delPlain(keys ...string) (int, error) {
	v, err := c.execWithKey(true, "DEL", keys[0], vstr2iface(keys[1:])...)
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/discard
// TODO: Discard

// http://redis.io/commands/dump
func (c *Client) Dump(key string) (string, error) {
	v, err := c.execWithKey(true, "DUMP", key)
	if err != nil {
		return "", err
	}
	return iface2str(v)
}

// http://redis.io/commands/echo
// Echo is not fully supported on sharded connections.
func (c *Client) Echo(message string) (string, error) {
	v, err := c.execWithKey(true, "ECHO", message)
	if err != nil {
		return "", err
	}
	return iface2str(v)
}

// http://redis.io/commands/eval
// Eval is not fully supported on sharded connections.
func (c *Client) Eval(script string, numkeys int, keys, args []string) (interface{}, error) {
	a := []interface{}{
		"EVAL",
		script, // escape?
		numkeys,
		strings.Join(keys, " "),
		strings.Join(args, " "),
	}
	v, err := c.execOnFirst(true, a...)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// http://redis.io/commands/evalsha
// EvalSha is not fully supported on sharded connections.
func (c *Client) EvalSha(sha1 string, numkeys int, keys, args []string) (interface{}, error) {
	a := []interface{}{
		"EVALSHA",
		sha1,
		numkeys,
		strings.Join(keys, " "),
		strings.Join(args, " "),
	}
	v, err := c.execOnFirst(true, a...)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// http://redis.io/commands/exec
// TODO: Exec

// http://redis.io/commands/exists
func (c *Client) Exists(key string) (bool, error) {
	v, err := c.execWithKey(true, "EXISTS", key)
	if err != nil {
		return false, err
	}
	return iface2bool(v)
}

// http://redis.io/commands/expire
// Expire returns true if the timeout was set, or false if key does not exist
// or the timeout could not be set.
func (c *Client) Expire(key string, seconds int) (bool, error) {
	v, err := c.execWithKey(true, "EXPIRE", key, seconds)
	if err != nil {
		return false, err
	}
	return iface2bool(v)
}

// http://redis.io/commands/expireat
// ExpireAt returns like Expire.
func (c *Client) ExpireAt(key string, timestamp int) (bool, error) {
	v, err := c.execWithKey(true, "EXPIREAT", key, timestamp)
	if err != nil {
		return false, err
	}
	return iface2bool(v)
}

// http://redis.io/commands/flushall
// FlushAll is not fully supported on sharded connections.
func (c *Client) FlushAll() error {
	_, err := c.execOnFirst(false, "FLUSHALL")
	return err
}

// http://redis.io/commands/flushall
// FlushDB is not fully supported on sharded connections.
func (c *Client) FlushDB() error {
	_, err := c.execOnFirst(false, "FLUSHDB")
	return err
}

// http://redis.io/commands/get
func (c *Client) Get(key string) (string, error) {
	v, err := c.execWithKey(true, "GET", key)
	if err != nil {
		return "", err
	}
	return iface2str(v)
}

// http://redis.io/commands/getbit
func (c *Client) GetBit(key string, offset int) (int, error) {
	v, err := c.execWithKey(true, "GETBIT", key, offset)
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/getrange
func (c *Client) GetRange(key string, start, end int) (string, error) {
	v, err := c.execWithKey(true, "GETRANGE", key, start, end)
	if err != nil {
		return "", err
	}
	switch v.(type) {
	case string:
		return v.(string), nil
	}
	return "", ErrServerError
}

// http://redis.io/commands/getset
func (c *Client) GetSet(key, value string) (string, error) {
	v, err := c.execWithKey(true, "GETSET", key, value)
	if err != nil {
		return "", err
	}
	return iface2str(v)
}

// http://redis.io/commands/incr
func (c *Client) Incr(key string) (int, error) {
	v, err := c.execWithKey(true, "INCR", key)
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/incrby
func (c *Client) IncrBy(key string, increment int) (int, error) {
	v, err := c.execWithKey(true, "INCRBY", key, increment)
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/keys
func (c *Client) Keys(pattern string) ([]string, error) {
	keys := []string{}
	v, err := c.execOnFirst(true, "KEYS", pattern)
	if err != nil {
		return keys, err
	}
	return iface2vstr(v), nil
}

// http://redis.io/commands/lpush
func (c *Client) LPush(key string, values ...string) (int, error) {
	v, err := c.execWithKey(true, "LPUSH", key, vstr2iface(values)...)
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/lindex
func (c *Client) LIndex(key string, index int) (string, error) {
	v, err := c.execWithKey(true, "LINDEX", key, index)
	if err != nil {
		return "", err
	}
	return iface2str(v)
}

// http://redis.io/commands/lpop
func (c *Client) LPop(key string) (string, error) {
	v, err := c.execWithKey(true, "LPOP", key)
	if err != nil {
		return "", err
	}
	return iface2str(v)
}

// http://redis.io/commands/rpop
func (c *Client) RPop(key string) (string, error) {
	v, err := c.execWithKey(true, "RPOP", key)
	if err != nil {
		return "", err
	}
	return iface2str(v)
}

// http://redis.io/commands/llen
func (c *Client) LLen(key string) (int, error) {
	v, err := c.execWithKey(true, "LLEN", key)
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/ltrim
func (c *Client) LTrim(key string, begin, end int) (err error) {
	_, err = c.execWithKey(true, "LTRIM", key, begin, end)
	return err
}

// http://redis.io/commands/lrange
func (c *Client) LRange(key string, begin, end int) ([]string, error) {
	v, err := c.execWithKey(true, "LRANGE", key, begin, end)
	if err != nil {
		return []string{}, err
	}
	return iface2vstr(v), nil
}

// http://redis.io/commands/hget
func (c *Client) HGet(key, member string) (string, error) {
	v, err := c.execWithKey(true, "HGET", key, member)
	if err != nil {
		return "", err
	}
	return iface2str(v)
}

// http://redis.io/commands/hgetall
func (c *Client) HGetAll(key string) (map[string]string, error) {
	v, err := c.execWithKey(true, "HGETALL", key)
	if err != nil {
		return nil, err
	}
	return iface2strmap(v), nil
}

// http://redis.io/commands/hincrby
func (c *Client) HIncrBy(key string, field string, increment int) (int, error) {
	v, err := c.execWithKey(true, "HINCRBY", key, field, increment)
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/hmget
func (c *Client) HMGet(key string, field ...string) ([]string, error) {
	v, err := c.execWithKey(true, "HMGET", key, vstr2iface(field)...)
	if err != nil {
		return nil, err
	}
	return iface2vstr(v), nil
}

// http://redis.io/commands/hmset
func (c *Client) HMSet(key string, items map[string]string) (err error) {
	tmp := make([]interface{}, (len(items) * 2))
	idx := 0
	for k, v := range items {
		n := idx * 2
		tmp[n] = k
		tmp[n+1] = v
		idx++
	}
	_, err = c.execWithKey(true, "HMSET", key, tmp...)
	return
}

// http://redis.io/commands/hset
func (c *Client) HSet(key, field, value string) (err error) {
	_, err = c.execWithKey(true, "HSET", key, field, value)
	return
}

// http://redis.io/commands/zincrby
func (c *Client) ZIncrBy(key string, increment int, member string) (string, error) {
	v, err := c.execWithKey(true, "ZINCRBY", key, increment, member)
	if err != nil {
		return "", err
	}
	return iface2str(v)
}

// WIP (we stopped here)
// http://redis.io/commands/mget

// MGet is not fully supported on sharded connections.
// TODO: fix
func (c *Client) MGet(keys ...string) ([]string, error) {
	tmp := make([]interface{}, len(keys)+1)
	tmp[0] = "MGET"
	for n, k := range keys {
		tmp[n+1] = k
	}
	v, err := c.execOnFirst(true, tmp...)
	if err != nil {
		return nil, err
	}
	switch v.(type) {
	case []interface{}:
		items := v.([]interface{})
		resp := make([]string, len(items))
		for n, item := range items {
			switch item.(type) {
			case string:
				resp[n] = item.(string)
			}
		}
		return resp, nil
	}
	return nil, ErrServerError
}

// http://redis.io/commands/mset
// MSet is not fully supported on sharded connections.
// TODO: fix
func (c *Client) MSet(items map[string]string) error {
	tmp := make([]interface{}, (len(items)*2)+1)
	tmp[0] = "MSET"
	idx := 0
	for k, v := range items {
		n := idx * 2
		tmp[n+1] = k
		tmp[n+2] = v
		idx++
	}
	_, err := c.execOnFirst(true, tmp...)
	if err != nil {
		return err
	}
	return nil
}

// http://redis.io/commands/publish
func (c *Client) Publish(channel string, value string) error {
	_, err := c.execWithKey(true, "PUBLISH", channel, value)
	return err
}

// http://redis.io/commands/rpush
func (c *Client) RPush(key string, values ...string) (int, error) {
	v, err := c.execWithKey(true, "RPUSH", key, vstr2iface(values)...)
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/sadd
func (c *Client) SAdd(key string, vs ...interface{}) (int, error) {
	v, err := c.execWithKey(true, "SADD", key, vs...)

	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/script-load
func (c *Client) ScriptLoad(script string) (string, error) {
	v, err := c.execOnFirst(true, "SCRIPT", "LOAD", script)
	if err != nil {
		return "", err
	}
	return iface2str(v)
}

// http://redis.io/commands/set
func (c *Client) Set(key, value string) (err error) {
	_, err = c.execWithKey(true, "SET", key, value)
	return
}

// http://redis.io/commands/setbit
func (c *Client) SetBit(key string, offset, value int) (int, error) {
	v, err := c.execWithKey(true, "SETBIT", key, offset, value)
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/setex
func (c *Client) SetEx(key string, seconds int, value string) (err error) {
	_, err = c.execWithKey(true, "SETEX", key, seconds, value)
	return
}

// http://redis.io/commands/smembers
func (c *Client) SMembers(key string) ([]string, error) {

	var v interface{}
	var err error

	v, err = c.execWithKey(true, "SMEMBERS", key)

	if err != nil {
		return []string{}, err
	}
	return iface2vstr(v), nil
}

type PubSubMessage struct {
	Error   error
	Value   string
	Channel string
}

// http://redis.io/commands/subscribe
func (c *Client) Subscribe(channel string, ch chan PubSubMessage, stop chan bool) error {
	srv, err := c.selector.PickServer("")

	if err != nil {
		return err
	}

	cn, err := c.getConn(srv)
	if err != nil {
		return err
	}

	_, err = c.execute(cn.rw, "SUBSCRIBE", channel)

	if err != nil {
		cn.condRelease(&err)
		return err
	}

	if err = cn.nc.SetDeadline(time.Time{}); err != nil {
		cn.condRelease(&err)
		return err
	}

	sibStop := make(chan bool)
	go func() {
		for {
			select {
			case <-stop:
				cn.nc.Close()
			case <-sibStop:
				return
			}
		}
	}()

	go func() {
		for {
			raw, err := c.parseResponse(cn.rw.Reader)
			if err != nil {
				msg := PubSubMessage{
					Error: err,
				}
				ch <- msg

				sibStop <- true
				cn.nc.Close()
				return
			}

			switch raw.(type) {
			case []interface{}:
				ret := raw.([]interface{})
				msg := PubSubMessage{
					Value:   ret[2].(string),
					Channel: ret[1].(string),
					Error:   nil,
				}
				ch <- msg
			default:
				msg := PubSubMessage{
					Error: errors.New("Protocol Error"),
				}
				ch <- msg
				sibStop <- true
				cn.nc.Close()
				return
			}
		}
	}()

	return err
}

// http://redis.io/commands/ttl
func (c *Client) TTL(key string) (int, error) {
	v, err := c.execWithKey(true, "TTL", key)
	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

func (c *Client) ZAdd(key string, vs ...interface{}) (int, error) {
	if len(vs)%2 != 0 {
		return 0, errors.New("Incomplete parameter sequence")
	}

	v, err := c.execWithKey(true, "ZADD", key, vs...)

	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

func (c *Client) ZCard(key string) (int, error) {
	v, err := c.execWithKey(true, "ZCARD", key)

	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

func (c *Client) ZCount(key string, min int, max int) (int, error) {
	v, err := c.execWithKey(true, "ZCOUNT", key, min, max)

	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// http://redis.io/commands/zrange
func (c *Client) ZRange(key string, start int, stop int, withscores bool) ([]string, error) {
	var v interface{}
	var err error

	if withscores == true {
		v, err = c.execWithKey(true, "ZRANGE", key, start, stop, "WITHSCORES")
	} else {
		v, err = c.execWithKey(true, "ZRANGE", key, start, stop)
	}

	if err != nil {
		return []string{}, err
	}
	return iface2vstr(v), nil
}

// http://redis.io/commands/zrevrange
func (c *Client) ZRevRange(key string, start int, stop int, withscores bool) ([]string, error) {
	var v interface{}
	var err error

	if withscores == true {
		v, err = c.execWithKey(true, "ZREVRANGE", key, start, stop, "WITHSCORES")
	} else {
		v, err = c.execWithKey(true, "ZREVRANGE", key, start, stop)
	}

	if err != nil {
		return []string{}, err
	}
	return iface2vstr(v), nil
}

// http://redis.io/commands/zscore
func (c *Client) ZScore(key string, member string) (string, error) {
	v, err := c.execWithKey(true, "ZSCORE", key, member)
	if err != nil {
		return "", err
	}
	return iface2str(v)
}

func (c *Client) ZRem(key string, vs ...interface{}) (int, error) {
	v, err := c.execWithKey(true, "ZREM", key, vs...)

	if err != nil {
		return 0, err
	}
	return iface2int(v)
}

// GetMulti is a batch version of Get. The returned map from keys to
// items may have fewer elements than the input slice, due to memcache
// cache misses. Each key must be at most 250 bytes in length.
// If no error is returned, the returned map will also be non-nil.
/*
func (c *Client) GetMulti(keys []string) (map[string]*Item, error) {
	var lk sync.Mutex
	m := make(map[string]*Item)
	addItemToMap := func(it *Item) {
		lk.Lock()
		defer lk.Unlock()
		m[it.Key] = it
	}

	keyMap := make(map[net.Addr][]string)
	for _, key := range keys {
		if !legalKey(key) {
			return nil, ErrMalformedKey
		}
		addr, err := c.selector.PickServer(key)
		if err != nil {
			return nil, err
		}
		keyMap[addr] = append(keyMap[addr], key)
	}

	ch := make(chan error, buffered)
	for addr, keys := range keyMap {
		go func(addr net.Addr, keys []string) {
			//ch <- c.getFromAddr(addr, keys, addItemToMap)
		}(addr, keys)
	}

	var err error
	for _ = range keyMap {
		if ge := <-ch; ge != nil {
			err = ge
		}
	}
	return m, err
}
*/
