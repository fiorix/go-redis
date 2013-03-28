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
func (c *Client) execWithKey(cmd, key string, a ...string) (v interface{}, err error) {
	addr, err := c.selector.PickServer(key)
	if err != nil {
		return
	}
	x := []string{cmd, key}
	return c.execWithAddr(addr, append(x, a...)...)
}

// executeWithKeys calls executeWithKey for each key, returns an array
func (c *Client) execWithKeys(cmd string, keys []string, a ...string) (v interface{}, err error) {
	var r []interface{}
	for _, k := range keys {
		if tmp, e := c.execWithKey(cmd, k, a...); e != nil {
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
func (c *Client) execWithAddr(addr net.Addr, a ...string) (v interface{}, err error) {
	cn, err := c.getConn(addr)
	if err != nil {
		return
	}
	defer cn.condRelease(&err)
	return c.execute(cn.rw, a...)
}

// execute sends a command to redis, then reads and parses the response.
// It's the redis wire protocol: http://redis.io/topics/protocol
func (c *Client) execute(rw *bufio.ReadWriter, a ...string) (v interface{}, err error) {
	fmt.Printf("Sending: %#v\n", a)
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
	reply := byte(line[0])
	lineLen := len(line)
	crlf := []byte("\r\n")
	if len(line) > 2 && bytes.Equal(line[lineLen-2:], crlf) {
		line = line[1 : lineLen-2]
	}
	switch reply {
	case '-':
		err = errors.New(string(line))
		return
	case '+':
		v = string(line)
		return
	case ':':
		response, e := strconv.Atoi(string(line))
		if e != nil {
			err = e
			return
		}
		v = response
	case '$':
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
	case '*':
		fmt.Println("It's an asterisk!", line)
	default:
		fmt.Println("Unexpected line:", string(line))
	}

	return
}

// Redis commands
// vvvvvvvvvvvvvv

// Append appends a value to a key, and returns the length of the new value.
// http://redis.io/commands/append
func (c *Client) Append(key, value string) (int, error) {
	n, err := c.execWithKey("append", key, value)
	if err != nil {
		return 0, err
	}
	switch n.(type) {
	case int:
		return n.(int), nil
	}
	return 0, ErrServerError
}

// Auth tries to authenticate the client by sending a password to the server.
// http://redis.io/commands/auth
//
// Auth is only executed in a specific connection of the Client instance.
// TODO: automatically call Auth on new connections
func (c *Client) Auth(addr net.Addr, passwd string) error {
	ok, err := c.execWithAddr(addr, "auth", passwd)
	if err != nil {
		return err
	}
	switch ok.(type) {
	case string:
		return nil
	}
	return ErrServerError
}

// BgRewriteAOF
// http://redis.io/commands/bgrewriteaof
//
// BgRewriteAOF is only executed in the control server.
// See selector.PickControlServer for details.
func (c *Client) BgRewriteAOF() (string, error) {
	addr, err := c.selector.PickControlServer()
	if err != nil {
		return "", err
	}
	status, err := c.execWithAddr(addr, "bgrewriteaof")
	if err != nil {
		return "", err
	}
	switch status.(type) {
	case string:
		return status.(string), nil
	}
	return "", ErrServerError
}

// BgSave
// http://redis.io/commands/bgsave
//
// BgSave is only executed in the control server.
// See selector.PickControlServer for details.
func (c *Client) BgSave() (string, error) {
	addr, err := c.selector.PickControlServer()
	if err != nil {
		return "", err
	}
	status, err := c.execWithAddr(addr, "bgsave")
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
		n, err = c.execWithKey("bitcount", key,
			fmt.Sprintf("%d", start), fmt.Sprintf("%d", end))
	} else {
		n, err = c.execWithKey("bitcount", key)
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
// BitOp does not work if the client is connected to different servers.
func (c *Client) BitOp(operation, destkey, key string, keys ...string) (int, error) {
	addr, err := c.selector.PickServer(key)
	if err != nil {
		return 0, err
	}
	a := []string{"bitop", operation, destkey, key}
	n, err := c.execWithAddr(addr, append(a, keys...)...)
	if err != nil {
		return 0, err
	}
	switch n.(type) {
	case int:
		return n.(int), nil
	}
	return 0, ErrServerError
}

// WIP

// Get gets the item for the given key. ErrCacheMiss is returned for a
// memcache cache miss. The key must be at most 250 bytes in length.
func (c *Client) Get(key string) (string, error) {
	value, err := c.execWithKey("get", key)
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
	_, err = c.execWithKey("set", key, value)
	return
}

// Add writes the given item, if no value already exists for its
// key. ErrNotStored is returned if that condition is not met.
func (c *Client) Add(key, value string) (err error) {
	_, err = c.execWithKey("add", key, value)
	return
}

// Delete deletes the item with the provided key. The error ErrCacheMiss is
// returned if the item didn't already exist in the cache.
func (c *Client) Delete(keys []string) (int, error) {
	v, err := c.execWithKeys("del", keys)
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
