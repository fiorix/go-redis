/*
Copyright 2011 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package memcache provides a client for the memcached cache server.
package redis

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Similar to:
// http://code.google.com/appengine/docs/go/memcache/reference.html

var (
	// ErrCacheMiss means that a Get failed because the item wasn't present.
	ErrCacheMiss = errors.New("memcache: cache miss")

	// ErrCASConflict means that a CompareAndSwap call failed due to the
	// cached value being modified between the Get and the CompareAndSwap.
	// If the cached value was simply evicted rather than replaced,
	// ErrNotStored will be returned instead.
	ErrCASConflict = errors.New("memcache: compare-and-swap conflict")

	// ErrNotStored means that a conditional write operation (i.e. Add or
	// CompareAndSwap) failed because the condition was not satisfied.
	ErrNotStored = errors.New("memcache: item not stored")

	// ErrServer means that a server error occurred.
	ErrServerError = errors.New("memcache: server error")

	// ErrNoStats means that no statistics were available.
	ErrNoStats = errors.New("memcache: no statistics available")

	// ErrMalformedKey is returned when an invalid key is used.
	// Keys must be at maximum 250 bytes long, ASCII, and not
	// contain whitespace or control characters.
	ErrMalformedKey = errors.New("malformed: key is too long or contains invalid characters")

	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("memcache: no servers configured or available")

	// ErrTimedOut is returned when a Read or Write operation times out
	ErrTimedOut = errors.New("redis: timed out")
)

// DefaultTimeout is the default socket read/write timeout.
const DefaultTimeout = time.Duration(100) * time.Millisecond

const (
	buffered            = 8 // arbitrary buffered channel size, for readability
	maxIdleConnsPerAddr = 2 // TODO(bradfitz): make this configurable?
)

// resumableError returns true if err is only a protocol-level cache error.
// This is used to determine whether or not a server connection should
// be re-used or not. If an error occurs, by default we don't reuse the
// connection, unless it was just a cache error.
func resumableError(err error) bool {
	switch err {
	case ErrCacheMiss, ErrCASConflict, ErrNotStored, ErrMalformedKey:
		return true
	}
	return false
}

func legalKey(key string) bool {
	if len(key) > 250 {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] <= ' ' || key[i] > 0x7e {
			return false
		}
	}
	return true
}

var (
	crlf            = []byte("\r\n")
	resultStored    = []byte("STORED\r\n")
	resultNotStored = []byte("NOT_STORED\r\n")
	resultExists    = []byte("EXISTS\r\n")
	resultNotFound  = []byte("NOT_FOUND\r\n")
	resultDeleted   = []byte("DELETED\r\n")
	resultEnd       = []byte("END\r\n")

	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
)

// New returns a memcache client using the provided server(s)
// with equal weight. If a server is listed multiple times,
// it gets a proportional amount of weight.
func New(server ...string) *Client {
	ss := new(ServerList)
	ss.SetServers(server...)
	return NewFromSelector(ss)
}

// NewFromSelector returns a new Client using the provided ServerSelector.
func NewFromSelector(ss ServerSelector) *Client {
	return &Client{selector: ss}
}

// Client is a memcache client.
// It is safe for unlocked use by multiple concurrent goroutines.
type Client struct {
	// Timeout specifies the socket read/write timeout.
	// If zero, DefaultTimeout is used.
	Timeout time.Duration

	selector ServerSelector

	lk       sync.Mutex
	freeconn map[net.Addr][]*conn
}

// conn is a connection to a server.
type conn struct {
	nc   net.Conn
	rw   *bufio.ReadWriter
	addr net.Addr
	c    *Client
}

// release returns this connection back to the client's free pool
func (cn *conn) release() {
	cn.c.putFreeConn(cn.addr, cn)
}

func (cn *conn) extendDeadline() {
	cn.nc.SetDeadline(time.Now().Add(cn.c.netTimeout()))
}

// condRelease releases this connection if the error pointed to by err
// is is nil (not an error) or is only a protocol level error (e.g. a
// cache miss).  The purpose is to not recycle TCP connections that
// are bad.
func (cn *conn) condRelease(err *error) {
	if *err == nil || resumableError(*err) {
		cn.release()
	} else {
		cn.nc.Close()
	}
}

func (c *Client) putFreeConn(addr net.Addr, cn *conn) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		c.freeconn = make(map[net.Addr][]*conn)
	}
	freelist := c.freeconn[addr]
	if len(freelist) >= maxIdleConnsPerAddr {
		cn.nc.Close()
		return
	}
	c.freeconn[addr] = append(freelist, cn)
}

func (c *Client) getFreeConn(addr net.Addr) (cn *conn, ok bool) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		return nil, false
	}
	freelist, ok := c.freeconn[addr]
	if !ok || len(freelist) == 0 {
		return nil, false
	}
	cn = freelist[len(freelist)-1]
	c.freeconn[addr] = freelist[:len(freelist)-1]
	return cn, true
}

func (c *Client) netTimeout() time.Duration {
	if c.Timeout != 0 {
		return c.Timeout
	}
	return DefaultTimeout
}

// ConnectTimeoutError is the error type used when it takes
// too long to connect to the desired host. This level of
// detail can generally be ignored.
type ConnectTimeoutError struct {
	Addr net.Addr
}

func (cte *ConnectTimeoutError) Error() string {
	return "memcache: connect timeout to " + cte.Addr.String()
}

func (c *Client) dial(addr net.Addr) (net.Conn, error) {
	type connError struct {
		cn  net.Conn
		err error
	}
	ch := make(chan connError)
	go func() {
		nc, err := net.Dial(addr.Network(), addr.String())
		ch <- connError{nc, err}
	}()
	select {
	case ce := <-ch:
		return ce.cn, ce.err
	case <-time.After(c.netTimeout()):
		// Too slow. Fall through.
	}
	// Close the conn if it does end up finally coming in
	go func() {
		ce := <-ch
		if ce.err == nil {
			ce.cn.Close()
		}
	}()
	return nil, &ConnectTimeoutError{addr}
}

func (c *Client) getConn(addr net.Addr) (*conn, error) {
	cn, ok := c.getFreeConn(addr)
	if ok {
		cn.extendDeadline()
		return cn, nil
	}
	nc, err := c.dial(addr)
	if err != nil {
		return nil, err
	}
	cn = &conn{
		nc:   nc,
		addr: addr,
		rw:   bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
		c:    c,
	}
	cn.extendDeadline()
	return cn, nil
}

// executeWithKey picks a server based on the key, and executes a command in redis.
func (c *Client) execWithKey(urp bool, cmd, key string, a ...string) (v interface{}, err error) {
	addr, err := c.selector.PickServer(key)
	if err != nil {
		return
	}
	x := []string{cmd, key}
	return c.execWithAddr(urp, addr, append(x, a...)...)
}

// executeWithKeys calls executeWithKey for each key, returns an array
func (c *Client) execWithKeys(urp bool, cmd string, keys []string, a ...string) (v interface{}, err error) {
	var r []interface{}
	for _, k := range keys {
		if tmp, e := c.execWithKey(urp, cmd, k, a...); e != nil {
			err = e
			return
		} else {
			r = append(r, tmp)
		}
	}
	v = r
	return
}

// executeWithAddr executes a command in a specific redis server
func (c *Client) execWithAddr(urp bool, addr net.Addr, a ...string) (v interface{}, err error) {
	cn, err := c.getConn(addr)
	if err != nil {
		return
	}
	defer cn.condRelease(&err)
	return c.execute(urp, cn.rw, a...)
}

// execute sends a command to redis, then reads and parses the response.
// execute uses the old protocol, unless urp=true (Unified Request Protocol).
// URP is optional to support (old) commands list CLIENT LIST, CLIENT KILL.
// Redis wire protocol: http://redis.io/topics/protocol
func (c *Client) execute(urp bool, rw *bufio.ReadWriter, a ...string) (v interface{}, err error) {
	//fmt.Printf("\nSending: %#v\n", a)
	if urp {
		// Optional: Unified Request Protocol
		_, err = fmt.Fprintf(rw, "*%d\r\n", len(a))
		if err != nil {
			return
		}
		for _, item := range a {
			_, err = fmt.Fprintf(rw, "$%d\r\n%s\r\n", len(item), item)
			if err != nil {
				return
			}
		}
	} else {
		// Default: old redis protocol.
		_, err = fmt.Fprintf(rw, strings.Join(a, " ")+"\r\n")
		if err != nil {
			return
		}
	}
	err = rw.Flush()
	if err != nil {
		return
	}
	return c.parseResponse(rw)
}

// parseResponse reads and parses responses from redis after executing a command.
func (c *Client) parseResponse(rw *bufio.ReadWriter) (v interface{}, err error) {
	line, e := rw.ReadSlice('\n')
	if err != nil {
		err = e
		return
	}
	//fmt.Printf("line=%#v err=%#v\n", string(line), err)
	if len(line) < 1 {
		err = ErrTimedOut
		return
	}
	reply := byte(line[0])
	lineLen := len(line)
	crlf := []byte("\r\n")
	if len(line) > 2 && bytes.Equal(line[lineLen-2:], crlf) {
		line = line[1 : lineLen-2]
	}
	switch reply {
	case '-': // Error reply
		err = errors.New(string(line))
		return
	case '+': // Status reply
		v = string(line)
		return
	case ':': // Integer reply
		response, e := strconv.Atoi(string(line))
		if e != nil {
			err = e
			return
		}
		v = response
	case '$': // Bulk reply
		valueLen, e := strconv.Atoi(string(line))
		if e != nil {
			err = e
			return
		}
		if valueLen == -1 {
			err = ErrCacheMiss
			return
		}
		b := make([]byte, valueLen+2) // 2==crlf
		var s byte
		for n := 0; n < cap(b); n++ {
			s, err = rw.ReadByte()
			if err != nil {
				return
			}
			b[n] = s
		}
		if len(b) != cap(b) {
			err = errors.New(fmt.Sprintf("Unexpected response: %#v", string(line)))
			return
		}
		v = string(b[:valueLen]) // removes proto trailing crlf
		return
	case '*': // Multi-bulk reply
		//fmt.Printf("multibulk line=%#v\n", line)
		nitems, e := strconv.Atoi(string(line))
		if e != nil {
			err = e
			return
		}
		if nitems < 1 {
			v = nil
			return
		}
		resp := make([]interface{}, nitems)
		for n := 0; n < nitems; n++ {
			resp[n], err = c.parseResponse(rw)
			if err != nil {
				return
			}
		}
		//fmt.Printf("multibulk=%#v\n", resp)
		v = resp
		return
	default:
		fmt.Println("Unexpected line:", string(line))
	}

	return
}

// Redis commands
//
// Some commands take an integer timeout, in seconds. It's not a time.Duration
// because redis only supports second resolution for timeouts.
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

// Append appends a value to a key, and returns the length of the new value.
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

// http://redis.io/commands/auth
// Auth is only executed in a specific connection of the Client instance.
// TODO: automatically call Auth on new connections
func (c *Client) Auth(addr net.Addr, passwd string) error {
	ok, err := c.execWithAddr(true, addr, "auth", passwd)
	if err != nil {
		return err
	}
	switch ok.(type) {
	case string:
		return nil
	}
	return ErrServerError
}

// http://redis.io/commands/bgrewriteaof
// BgRewriteAOF does not work on sharded connections.
func (c *Client) BgRewriteAOF() (string, error) {
	addr, err := c.selector.PickControlServer()
	if err != nil {
		return "", err
	}
	status, err := c.execWithAddr(false, addr, "bgrewriteaof")
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
// BgSave does not work on sharded connections.
func (c *Client) BgSave() (string, error) {
	addr, err := c.selector.PickControlServer()
	if err != nil {
		return "", err
	}
	status, err := c.execWithAddr(false, addr, "bgsave")
	if err != nil {
		return "", err
	}
	switch status.(type) {
	case string:
		return status.(string), nil
	}
	return "", ErrServerError
}

// BitCount counts the number of set bits (population counting) in a string.
// http://redis.io/commands/bitcount
//
// BitCount will not send start and end if start is a negative number.
func (c *Client) BitCount(key string, start, end int) (int, error) {
	// TODO: move int convertions to .execute (it should take interface{})
	var (
		n   interface{}
		err error
	)
	if start > -1 {
		n, err = c.execWithKey(true, "bitcount", key,
			fmt.Sprintf("%d", start), fmt.Sprintf("%d", end))
	} else {
		n, err = c.execWithKey(true, "bitcount", key)
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

// BitOp performs a bitwise operation between multiple keys and store the
// result in the destination.
// http://redis.io/commands/bitop
//
// BitOp does not work on sharded connections.
func (c *Client) BitOp(operation, destkey, key string, keys ...string) (int, error) {
	addr, err := c.selector.PickServer(key)
	if err != nil {
		return 0, err
	}
	a := []string{"bitop", operation, destkey, key}
	n, err := c.execWithAddr(true, addr, append(a, keys...)...)
	if err != nil {
		return 0, err
	}
	switch n.(type) {
	case int:
		return n.(int), nil
	}
	return 0, ErrServerError
}

// blbrPop is a generic function used by both BLPop and BRPop.
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
		k = fmt.Sprintf("%s", items[0])
		v = fmt.Sprintf("%s", items[1])
		return
	}
	err = ErrServerError
	return
}

// http://redis.io/commands/blpop
// BLPop does not work on sharded connections.
func (c *Client) BLPop(timeout int, keys ...string) (k, v string, err error) {
	return c.blbrPop("blpop", timeout, keys...)
}

// http://redis.io/commands/brpop
// BRPop does not work on sharded connections.
func (c *Client) BRPop(timeout int, keys ...string) (k, v string, err error) {
	return c.blbrPop("brpop", timeout, keys...)
}

// http://redis.io/commands/brpoplpush
// BRPopLPush does not work on sharded connections.
func (c *Client) BRPopLPush(src, dst string, timeout int) (string, error) {
	t := c.Timeout
	// Extend the client's timeout for this operation only.
	// TODO: make sure it does not affect other concurrent calls.
	if t != 0 {
		c.Timeout = time.Duration(timeout)*time.Second + t
	} else {
		c.Timeout = time.Duration(timeout)*time.Second + DefaultTimeout
	}
	resp, err := c.execWithKey(true, "brpoplpush", src,
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
// ClientKill does not work on sharded connections.
func (c *Client) ClientKill(kill_addr string) error {
	addr, err := c.selector.PickControlServer()
	if err != nil {
		return err
	}
	ok, err := c.execWithAddr(false, addr,
		fmt.Sprintf("client kill %s", kill_addr))
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
// ClientList does not work on sharded connections.
func (c *Client) ClientList() ([]string, error) {
	addr, err := c.selector.PickControlServer()
	if err != nil {
		return nil, err
	}
	items, err := c.execWithAddr(false, addr, "client list")
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
// ClientSetName does not work on sharded connections, and is useless here.
// This driver creates connections on demand, thus naming them is pointless.
func (c *Client) ClientSetName(name string) error {
	addr, err := c.selector.PickControlServer()
	if err != nil {
		return err
	}
	ok, err := c.execWithAddr(false, addr,
		fmt.Sprintf("client setname %s", name))
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
// ConfigGet does not work on sharded connections.
func (c *Client) ConfigGet(name string) (map[string]string, error) {
	addr, err := c.selector.PickControlServer()
	if err != nil {
		return nil, err
	}
	resp, err := c.execWithAddr(false, addr,
		fmt.Sprintf("config get %s", name))
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
// ConfigSet does not work on sharded connections.
func (c *Client) ConfigSet(name, value string) error {
	addr, err := c.selector.PickControlServer()
	if err != nil {
		return err
	}
	ok, err := c.execWithAddr(false, addr,
		fmt.Sprintf("config set %s %s", name, value))
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
// ConfigResetStat does not work on sharded connections.
func (c *Client) ConfigResetStat() error {
	addr, err := c.selector.PickControlServer()
	if err != nil {
		return err
	}
	ok, err := c.execWithAddr(false, addr, "config resetstat")
	if err != nil {
		return err
	}
	switch ok.(type) {
	case string:
		return nil
	}
	return ErrServerError
}

// WIP

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (c *Client) Get(key string) (string, error) {
	value, err := c.execWithKey(true, "get", key)
	if err != nil {
		return "", err
	}
	return value.(string), err
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

// Set writes the given item, unconditionally.
func (c *Client) Set(key, value string) (err error) {
	_, err = c.execWithKey(true, "set", key, value)
	return
}

// Add writes the given item, if no value already exists for its
// key. ErrNotStored is returned if that condition is not met.
func (c *Client) Add(key, value string) (err error) {
	_, err = c.execWithKey(true, "add", key, value)
	return
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (c *Client) Delete(keys ...string) (int, error) {
	v, err := c.execWithKeys(true, "del", keys)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, n := range v.([]interface{}) {
		deleted += n.(int)
	}
	return deleted, nil
}

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

// RPush appends values at the tail of the list stored at key.
// RPush returns the length of the list after the push operation.
// http://redis.io/commands/rpush
func (c *Client) RPush(key string, values ...string) (int, error) {
	n, err := c.execWithKey(true, "rpush", key, values...)
	if err != nil {
		return 0, err
	}
	switch n.(type) {
	case int:
		return n.(int), nil
	}
	return 0, ErrServerError
}
