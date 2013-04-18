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
// Package redis provides a client for the redis cache server.

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

var (
	// ErrNoServers is returned when no servers are configured or available.
	ErrNoServers = errors.New("redis: no servers configured or available")

	// ErrServer means that a server error occurred.
	ErrServerError = errors.New("redis: server error")

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
	case ErrTimedOut, ErrServerError:
		return false
	}
	return true
}

// New returns a redis client using the provided server(s) with equal weight.
// If a server is listed multiple times, it gets a proportional amount of
// weight.
//
// New supports ip:port, unix:/path, and optional db=N and passwd=PWD.
//
// Example:
//
//	rc := redis.New("ip:port db=N passwd=foobared")
func New(server ...string) *Client {
	ss := new(ServerList)
	if err := ss.SetServers(server...); err != nil {
		panic(err)
	}
	return NewFromSelector(ss)
}

// NewFromSelector returns a new Client using the provided ServerSelector.
func NewFromSelector(ss ServerSelector) *Client {
	return &Client{selector: ss}
}

// Client is a redis client.
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
	nc  net.Conn
	rw  *bufio.ReadWriter
	srv ServerInfo
	c   *Client
}

// release returns this connection back to the client's free pool
func (cn *conn) release() {
	cn.c.putFreeConn(cn.srv.Addr, cn)
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

func (c *Client) getFreeConn(srv ServerInfo) (cn *conn, ok bool) {
	c.lk.Lock()
	defer c.lk.Unlock()
	if c.freeconn == nil {
		return nil, false
	}
	freelist, ok := c.freeconn[srv.Addr]
	if !ok || len(freelist) == 0 {
		return nil, false
	}
	cn = freelist[len(freelist)-1]
	c.freeconn[srv.Addr] = freelist[:len(freelist)-1]
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
	return "redis: connect timeout to " + cte.Addr.String()
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

func (c *Client) getConn(srv ServerInfo) (*conn, error) {
	cn, ok := c.getFreeConn(srv)
	if ok {
		cn.extendDeadline()
		return cn, nil
	}
	nc, err := c.dial(srv.Addr)
	if err != nil {
		return nil, err
	}
	cn = &conn{
		nc:  nc,
		srv: srv,
		rw:  bufio.NewReadWriter(bufio.NewReader(nc), bufio.NewWriter(nc)),
		c:   c,
	}
	cn.extendDeadline()
	if srv.Passwd != "" {
		_, err := c.execute(true, cn.rw, "AUTH", srv.Passwd)
		if err != nil {
			return nil, err
		}
	}
	if srv.DB != "" {
		_, err := c.execute(false, cn.rw, "SELECT", srv.DB)
		if err != nil {
			return nil, err
		}
	}
	return cn, nil
}

// vstr2iface converts an array of strings to an array of empty interfaces
func vstr2iface(a []string) (r []interface{}) {
	r = make([]interface{}, len(a))
	for n, item := range a {
		r[n] = item
	}
	return
}

// iface2bool validates and converts interface (int) to bool
func iface2bool(a interface{}) (bool, error) {
	switch a.(type) {
	case int:
		if a.(int) == 1 {
			return true, nil
		} else {
			return false, nil
		}
	}
	return false, ErrServerError
}

// iface2int validates and converts interface to int
func iface2int(a interface{}) (int, error) {
	switch a.(type) {
	case int:
		return a.(int), nil
	}
	return 0, ErrServerError
}

// iface2str validates and converts interface to string
func iface2str(a interface{}) (string, error) {
	switch a.(type) {
	case string:
		return a.(string), nil
	}
	return "", ErrServerError
}

// execWithKey picks a server based on the key, and executes a command in redis.
func (c *Client) execWithKey(urp bool, cmd, key string, a ...interface{}) (v interface{}, err error) {
	srv, err := c.selector.PickServer(key)
	if err != nil {
		return
	}
	x := []interface{}{cmd, key}
	return c.execWithAddr(urp, srv, append(x, a...)...)
}

// execWithKeys calls execWithKey for each key, returns an array of results.
func (c *Client) execWithKeys(urp bool, cmd string, keys []string, a ...interface{}) (v interface{}, err error) {
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

// execOnFirst executes a command on the first listed server.
// execOnFirst is used by commands that are not bound to a key. e.g.: ping, info
func (c *Client) execOnFirst(urp bool, a ...interface{}) (interface{}, error) {
	srv, err := c.selector.PickServer("")
	if err != nil {
		return nil, err
	}
	return c.execWithAddr(urp, srv, a...)
}

// execWithAddr executes a command in a specific redis server.
func (c *Client) execWithAddr(urp bool, srv ServerInfo, a ...interface{}) (v interface{}, err error) {
	cn, err := c.getConn(srv)
	if err != nil {
		return
	}
	defer cn.condRelease(&err)
	return c.execute(urp, cn.rw, a...)
}

// execute sends a command to redis, then reads and parses the response.
// It uses the old protocol, unless urp (Unified Request Protocol) is true.
// Redis protocol <http://redis.io/topics/protocol>
func (c *Client) execute(urp bool, rw *bufio.ReadWriter, a ...interface{}) (v interface{}, err error) {
	//fmt.Printf("\nSending: %#v\n", a)
	s := make([]string, len(a))
	for n, item := range a {
		switch item.(type) {
		case int:
			s[n] = strconv.Itoa(item.(int))
		case string:
			s[n] = item.(string)
		default:
			// TODO: panic?
		}
	}
	if urp {
		// Optional: Unified Request Protocol
		_, err = fmt.Fprintf(rw, "*%d\r\n", len(a))
		if err != nil {
			return
		}
		for _, i := range s {
			_, err = fmt.Fprintf(rw, "$%d\r\n%s\r\n", len(i), i)
			if err != nil {
				return
			}
		}
	} else {
		// Default: old redis protocol.
		_, err = fmt.Fprintf(rw, strings.Join(s, " ")+"\r\n")
		if err != nil {
			return
		}
	}
	if err = rw.Flush(); err != nil {
		return
	}
	return c.parseResponse(rw)
}

// parseResponse reads and parses a single response from redis.
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
	if len(line) > 2 && bytes.Equal(line[lineLen-2:], []byte("\r\n")) {
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
			v = "" // err = ErrCacheMiss
			return
		}
		b := make([]byte, valueLen+2) // 2==crlf, TODO: fix this
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
		// TODO: return error and kill it
		fmt.Println("Unexpected line:", string(line))
	}

	return
}
