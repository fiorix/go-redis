go-redis
========

go-redis is a [Redis](http://redis.io) client library for the
[Go programming language](http://golang.org). It's built on the skeleton of
[gomemcache](http://github.com/bradfitz/gomemcache).

It is safe to use by multiple goroutines, and scales well by automatically
making new connections to redis on demand. Idle connections stay in the
connection pool until time out.

Licensed under the Apache License, Version 2.0.


## Status

The library is stable. It was the quota backend for the freegeoip.net
service (now retired) and handled billions of queries in production
over its lifetime.

Coverage is partial: only the commands needed by the original authors
and contributors have been implemented. See
[commands.go](https://github.com/fiorix/go-redis/blob/master/redis/commands.go)
for the current list, in alphabetical order. Pull requests are welcome.

[![CI](https://github.com/fiorix/go-redis/actions/workflows/ci.yml/badge.svg)](https://github.com/fiorix/go-redis/actions/workflows/ci.yml)


## Installing

Requires Go 1.23 or newer. In a module-enabled project:

	$ go get github.com/fiorix/go-redis/redis


## Usage

Hello world:

	import "github.com/fiorix/go-redis/redis"

	func main() {
		rc := redis.New("10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379")
		rc.Set("foo", "bar")

		v, err := rc.Get("foo")
		...
	}

When connected to multiple servers, keyed commands (GET, SET, etc.)
are sharded across the configured servers using CRC32 of the key.
Commands that are not bound to a key (PING, INFO, and similar) run
on the first listed server only; they are marked "Cannot be sharded"
in their doc strings.

New connections are created on demand and stay available in the
connection pool until they are closed or time out.


### Unix socket, dbid and password support

The client supports ip:port or unix socket for connecting to redis.

	rc := redis.New("/tmp/redis.sock db=5 passwd=foobared")

Database ID and password can only be set by ``New()`` and can't be
changed later. If that is required, make a new connection.


## Credits

Thanks to (in no particular order):

- [gomemcache](https://github.com/bradfitz/gomemcache): for the skeleton of
this client library.
