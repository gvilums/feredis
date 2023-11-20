# feredis
The `feredis` project contains a set of crates that implement the redis wire protocol,
as well as a simple redis-compatible server.

It was created as a project to learn more about how async rust works under the hood,
using custom `Future`s to handle real-time key expiration.

## Supported Features
The `feredis` server implements the following common redis commands:
- `PING`
- `SET`
- `GET`
- `DEL`
- `EXPIRE`
- `PERSIST`
- `RENAME`
- `RPUSH`
- `RPOP`

## License
`feredis` is dual-licensed under the Apache License version 2.0 and the MIT license, at your choosing.
