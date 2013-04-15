// Copyright 2013 Alexandre Fiori
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
// Redis allows clients to block indefinetely by setting timeout to 0, but
// it does not work here. All functions below use the timeout not only to
// block the operation in redis, but also as a socket read timeout (+delta)
// to free up system resources.
//
// The default TCP read timeout is 100ms.
//
// If a timeout is required to be "indefinetely", then set it to 24h-ish.
// 🍺

import (
	"fmt"
	"strings"
	"time"
)

// http://redis.io/commands/append
func (c *Client) Append(key, value string) (int, error) {
	n, err := c.execWithKey(true, "append", key, value)
	if err != nil {
		return 0, err
	}
	switch n.(type) {
	case int:
		return n.(int), nil
	}
	return 0, ErrServerError
}

// http://redis.io/commands/bgrewriteaof
// BgRewriteAOF is not fully supported on sharded connections.
func (c *Client) BgRewriteAOF() (string, error) {
	status, err := c.execOnFirst(false, "BGREWRITEAOF")
	if err != nil {
		return "", err
	}
	switch status.(type) {
	case string:
		return status.(string), nil
	}
	return "", ErrServerError
}

// http://redis.io/commands/bgsave
// BgSave is not fully supported on sharded connections.
func (c *Client) BgSave() (string, error) {
	status, err := c.execOnFirst(false, "BGSAVE")
	if err != nil {
		return "", err
	}
	switch status.(type) {
	case string:
		return status.(string), nil
	}
	return "", ErrServerError
}

// http://redis.io/commands/bitcount
// BitCount ignores start and end if start is a negative number.
func (c *Client) BitCount(key string, start, end int) (int, error) {
	// TODO: move int convertions to .execute (it should take interface{})
	var (
		n   interface{}
		err error
	)
	if start > -1 {
		n, err = c.execWithKey(true, "BITCOUNT", key,
			fmt.Sprintf("%d", start), fmt.Sprintf("%d", end))
	} else {
		n, err = c.execWithKey(true, "BITCOUNT", key)
	}
	if err != nil {
		return 0, err
	}
	switch n.(type) {
	case int:
		return n.(int), nil
	}
	return 0, ErrServerError
}

// http://redis.io/commands/bitop
// BitOp is not fully supported on sharded connections.
func (c *Client) BitOp(operation, destkey, key string, keys ...string) (int, error) {
	a := []string{"BITOP", operation, destkey, key}
	n, err := c.execOnFirst(true, append(a, keys...)...)
	if err != nil {
		return 0, err
	}
	switch n.(type) {
	case int:
		return n.(int), nil
	}
	return 0, ErrServerError
}

// blbrPop supports both BLPop and BRPop.
func (c *Client) blbrPop(cmd string, timeout int, keys ...string) (k, v string, err error) {
	t := c.Timeout
	keys = append(keys, fmt.Sprintf("%d", timeout))
	var resp interface{}
	// Extend the client's timeout for this operation only.
	// TODO: make sure it does not affect other concurrent calls.
	if t != 0 {
		c.Timeout = time.Duration(timeout) + t
	} else {
		c.Timeout = time.Duration(timeout) + DefaultTimeout
	}
	resp, err = c.execWithKey(true, cmd, keys[0], keys[1:]...)
	c.Timeout = t
	if err != nil {
		return
	}
	if resp == nil {
		err = ErrTimedOut
		return
	}
	switch resp.(type) {
	case []interface{}:
		items := resp.([]interface{})
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
	if t != 0 {
		c.Timeout = time.Duration(timeout)*time.Second + t
	} else {
		c.Timeout = time.Duration(timeout)*time.Second + DefaultTimeout
	}
	resp, err := c.execWithKey(true, "BRPOPLPUSH", src,
		[]string{dst, fmt.Sprintf("%d", timeout)}...)
	c.Timeout = t
	if err != nil {
		return "", err
	}
	if resp == nil {
		return "", ErrTimedOut
	}
	switch resp.(type) {
	case string:
		return resp.(string), nil
	}
	return "", ErrServerError
}

// http://redis.io/commands/client-kill
// ClientKill is not fully supported on sharded connections.
func (c *Client) ClientKill(kill_addr string) error {
	ok, err := c.execOnFirst(false, "CLIENT KILL", kill_addr)
	if err != nil {
		return err
	}
	switch ok.(type) {
	case string:
		return nil
	}
	return ErrServerError
}

// http://redis.io/commands/client-list
// ClientList is not fully supported on sharded connections.
func (c *Client) ClientList() ([]string, error) {
	items, err := c.execOnFirst(false, "CLIENT LIST")
	if err != nil {
		return nil, err
	}
	switch items.(type) {
	case string:
		return strings.Split(items.(string), "\n"), nil
	}
	return nil, ErrServerError
}

// http://redis.io/commands/client-setname
// ClientSetName is not fully supported on sharded connections, and is useless here.
// This driver creates connections on demand, thus naming them is pointless.
func (c *Client) ClientSetName(name string) error {
	ok, err := c.execOnFirst(false, "CLIENT SETNAME", name)
	if err != nil {
		return err
	}
	switch ok.(type) {
	case string:
		return nil
	}
	return ErrServerError
}

// http://redis.io/commands/config-get
// ConfigGet is not fully supported on sharded connections.
func (c *Client) ConfigGet(name string) (map[string]string, error) {
	resp, err := c.execOnFirst(false, "CONFIG GET", name)
	if err != nil {
		return nil, err
	}
	var items []interface{}
	switch resp.(type) {
	case []interface{}:
		items = resp.([]interface{})
	default:
		return nil, ErrServerError
	}
	k := ""
	v := ""
	m := make(map[string]string)
	for n, item := range items {
		switch item.(type) {
		case string:
			v = item.(string)
		default:
			return nil, ErrServerError
		}
		if n%2 == 0 {
			k = v
		} else if n%2 == 1 {
			m[k] = v
		}
	}
	m[k] = v
	return m, nil
}

// http://redis.io/commands/config-set
// ConfigSet is not fully supported on sharded connections.
func (c *Client) ConfigSet(name, value string) error {
	ok, err := c.execOnFirst(false, "CONFIG SET", name, value)
	if err != nil {
		return err
	}
	switch ok.(type) {
	case string:
		return nil
	}
	return ErrServerError
}

// http://redis.io/commands/config-resetstat
// ConfigResetStat is not fully supported on sharded connections.
func (c *Client) ConfigResetStat() error {
	ok, err := c.execOnFirst(false, "CONFIG RESETSTAT")
	if err != nil {
		return err
	}
	switch ok.(type) {
	case string:
		return nil
	}
	return ErrServerError
}

// http://redis.io/commands/dbsize
// DBSize is not fully supported on sharded connections.
func (c *Client) DBSize() (int, error) {
	size, err := c.execOnFirst(false, "DBSIZE")
	if err != nil {
		return 0, err
	}
	switch size.(type) {
	case int:
		return size.(int), nil
	}
	return 0, ErrServerError
}

// http://redis.io/commands/debug-segfault
// DebugSegfault is not fully supported on sharded connections.
func (c *Client) DebugSegfault() error {
	ok, err := c.execOnFirst(false, "DEBUG SEGFAULT")
	if err != nil {
		return err
	}
	switch ok.(type) {
	case string:
		return nil
	}
	return ErrServerError
}

// http://redis.io/commands/decr
func (c *Client) Decr(key string) (int, error) {
	n, err := c.execWithKey(true, "DECR", key)
	if err != nil {
		return 0, err
	}
	switch n.(type) {
	case int:
		return n.(int), nil
	}
	return 0, ErrServerError
}

// http://redis.io/commands/decrby
func (c *Client) DecrBy(key string, decrement int) (int, error) {
	n, err := c.execWithKey(true, "DECRBY", key, fmt.Sprintf("%d", decrement))
	if err != nil {
		return 0, err
	}
	switch n.(type) {
	case int:
		return n.(int), nil
	}
	return 0, ErrServerError
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
	n, err := c.execWithKey(true, "DEL", keys[0], keys[1:]...)
	if err != nil {
		return 0, err
	}
	switch n.(type) {
	case int:
		return n.(int), nil
	}
	return 0, ErrServerError
}

// http://redis.io/commands/discard
// TODO: Discard

// http://redis.io/commands/dump
func (c *Client) Dump(key string) (string, error) {
	v, err := c.execWithKey(true, "DUMP", key)
	if err != nil {
		return "", err
	}
	switch v.(type) {
	case string:
		return v.(string), nil
	}
	return "", ErrServerError
}

// http://redis.io/commands/echo
// Echo is not fully supported on sharded connections.
func (c *Client) Echo(message string) (string, error) {
	v, err := c.execWithKey(true, "ECHO", message)
	if err != nil {
		return "", err
	}
	switch v.(type) {
	case string:
		return v.(string), nil
	}
	return "", ErrServerError
}

// http://redis.io/commands/eval
// Eval is not fully supported on sharded connections.
func (c *Client) Eval(script string, numkeys int, keys []string, args []string) (interface{}, error) {
	a := []string{
		"EVAL",
		script, // escape?
		fmt.Sprintf("%d", numkeys),
		strings.Join(keys, " "),
		strings.Join(args, " "),
	}
	resp, err := c.execOnFirst(true, a...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// http://redis.io/commands/evalsha
// EvalSha is not fully supported on sharded connections.
func (c *Client) EvalSha(sha1 string, numkeys int, keys []string, args []string) (interface{}, error) {
	a := []string{
		"EVALSHA",
		sha1,
		fmt.Sprintf("%d", numkeys),
		strings.Join(keys, " "),
		strings.Join(args, " "),
	}
	resp, err := c.execOnFirst(true, a...)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// http://redis.io/commands/exec
// TODO: Exec

// http://redis.io/commands/exists
func (c *Client) Exists(key string) (bool, error) {
	n, err := c.execWithKey(true, "EXISTS", key)
	if err != nil {
		return false, err
	}
	switch n.(type) {
	case int:
		if n.(int) == 1 {
			return true, nil
		} else {
			return false, nil
		}
	}
	return false, ErrServerError
}

// http://redis.io/commands/expire
// Expire returns true if the timeout was set, or false if key does not exist
// or the timeout could not be set.
func (c *Client) Expire(key string, seconds int) (bool, error) {
	n, err := c.execWithKey(true, "EXPIRE", key, fmt.Sprintf("%d", seconds))
	if err != nil {
		return false, err
	}
	switch n.(type) {
	case int:
		if n.(int) == 1 {
			return true, nil
		} else {
			return false, nil
		}
	}
	return false, ErrServerError
}

// http://redis.io/commands/expireat
// ExpireAt returns like Expire.
func (c *Client) ExpireAt(key string, timestamp int) (bool, error) {
	n, err := c.execWithKey(true, "EXPIREAT", key, fmt.Sprintf("%d", timestamp))
	if err != nil {
		return false, err
	}
	switch n.(type) {
	case int:
		if n.(int) == 1 {
			return true, nil
		} else {
			return false, nil
		}
	}
	return false, ErrServerError
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
	switch v.(type) {
	case string:
		return v.(string), nil
	}
	return "", ErrServerError
}

// http://redis.io/commands/getbit
func (c *Client) GetBit(key string, offset int) (int, error) {
	v, err := c.execWithKey(true, "GETBIT", key, fmt.Sprintf("%d", offset))
	if err != nil {
		return 0, err
	}
	switch v.(type) {
	case int:
		return v.(int), nil
	}
	return 0, ErrServerError
}

// WIP

// http://redis.io/commands/mget
// MGet is not fully supported on sharded connections.
// TODO: fix
func (c *Client) MGet(keys ...string) ([]string, error) {
	tmp := make([]string, len(keys)+1)
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
	tmp := make([]string, (len(items)*2)+1)
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

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
// http://redis.io/commands/rpush
func (c *Client) RPush(key string, values ...string) (int, error) {
	n, err := c.execWithKey(true, "RPUSH", key, values...)
	if err != nil {
		return 0, err
	}
	switch n.(type) {
	case int:
		return n.(int), nil
	}
	return 0, ErrServerError
}

// http://redis.io/commands/script-load
func (c *Client) ScriptLoad(script string) (string, error) {
	v, err := c.execOnFirst(true, "SCRIPT", "LOAD", script)
	if err != nil {
		return "", err
	}
	switch v.(type) {
	case string:
		return v.(string), nil
	}
	return "", ErrServerError
}

// Set writes the given item, unconditionally.
func (c *Client) Set(key, value string) (err error) {
	_, err = c.execWithKey(true, "SET", key, value)
	return
}

// http://redis.io/commands/setbit
func (c *Client) SetBit(key string, offset, value int) (int, error) {
	v, err := c.execWithKey(true, "SETBIT", key,
		fmt.Sprintf("%d", offset), fmt.Sprintf("%d", value))
	if err != nil {
		return 0, err
	}
	switch v.(type) {
	case int:
		return v.(int), nil
	}
	return 0, ErrServerError
}

// http://redis.io/commands/ttl
func (c *Client) TTL(key string) (int, error) {
	n, err := c.execWithKey(true, "TTL", key)
	if err != nil {
		return 0, err
	}
	switch n.(type) {
	case int:
		return n.(int), nil
	}
	return 0, ErrServerError
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

// Increment atomically increments key by delta. The return value is
// the new value after being incremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On 64-bit overflow, the new value wraps around.
/*
func (c *Client) Increment(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("incr", key, delta)
}
*/

// Decrement atomically decrements key by delta. The return value is
// the new value after being decremented or an error. If the value
// didn't exist in memcached the error is ErrCacheMiss. The value in
// memcached must be an decimal number, or an error will be returned.
// On underflow, the new value is capped at zero and does not wrap
// around.
/*
func (c *Client) Decrement(key string, delta uint64) (newValue uint64, err error) {
	return c.incrDecr("decr", key, delta)
}

func (c *Client) incrDecr(verb, key string, delta uint64) (uint64, error) {
	var val uint64
	err := c.withKeyRw(key, func(rw *bufio.ReadWriter) error {
		line, err := writeReadLine(rw, "%s %s %d\r\n", verb, key, delta)
		if err != nil {
			return err
		}
		switch {
		case bytes.Equal(line, resultNotFound):
			return ErrCacheMiss
		case bytes.HasPrefix(line, resultClientErrorPrefix):
			errMsg := line[len(resultClientErrorPrefix) : len(line)-2]
			return errors.New("memcache: client error: " + string(errMsg))
		}
		val, err = strconv.ParseUint(string(line[:len(line)-2]), 10, 64)
		if err != nil {
			return err
		}
		return nil
	})
	return val, err
}
*/
