use std::io;

use std::net::TcpStream;

use smol::io::{AsyncBufReadExt, BufReader};
use smol::Async;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum RedisItem {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<RedisItem>),
    Null,
    Boolean(bool),
    // Double(f64),
}

impl RedisItem {
    pub fn serialize(&self, target: &mut Vec<u8>) {
        use RedisItem::*;
        match self {
            SimpleString(val) => {
                target.push(b'+');
                target.extend_from_slice(val.as_bytes());
                target.extend_from_slice(b"\r\n");
            }
            SimpleError(val) => {
                target.push(b'-');
                target.extend_from_slice(val.as_bytes());
                target.extend_from_slice(b"\r\n");
            }
            Integer(val) => {
                target.push(b':');
                target.extend_from_slice(val.to_string().as_bytes());
                target.extend_from_slice(b"\r\n");
            }
            BulkString(val) => {
                target.push(b'$');
                target.extend_from_slice(val.len().to_string().as_bytes());
                target.extend_from_slice(b"\r\n");
                target.extend_from_slice(val.as_bytes());
                target.extend_from_slice(b"\r\n");
            }
            Array(val) => {
                target.push(b'*');
                target.extend_from_slice(val.len().to_string().as_bytes());
                target.extend_from_slice(b"\r\n");
                for item in val {
                    item.serialize(target);
                }
            }
            Null => target.extend_from_slice(b"_\r\n"),
            Boolean(val) => {
                if *val {
                    target.extend_from_slice(b"#t\r\n");
                } else {
                    target.extend_from_slice(b"#f\r\n");
                }
            }
            // Double(val) => {
            //     target.push(b',');
            //     target.extend_from_slice(val.to_string().as_bytes());
            //     target.extend_from_slice(b"\r\n");
            // },
        }
    }
}

enum ParseState {
    List {
        remaining: usize,
        items: Vec<RedisItem>,
    },
    // Map {
    //     remaining: usize,
    //     items: HashMap<String, RedisItem>,
    // },
}

enum ParseResult {
    Partial(ParseState),
    Complete(RedisItem),
}

pub struct ItemParser {
    buffer: Vec<u8>,
    stack: Vec<ParseState>,
}

pub enum ParseError {
    Incomplete,
    Invalid,
    IoError(io::Error),
}

impl From<io::Error> for ParseError {
    fn from(err: io::Error) -> Self {
        Self::IoError(err)
    }
}

impl ItemParser {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            stack: Vec::new(),
        }
    }

    async fn parse_partial(
        &mut self,
        stream: &mut BufReader<&Async<TcpStream>>,
    ) -> Result<ParseResult, ParseError> {
        self.buffer.clear();
        let read0 = stream.read_until(b'\n', &mut self.buffer).await?;
        if read0 < 3 {
            return Err(ParseError::Incomplete);
        }
        match self.buffer[0] {
            b'_' => Ok(ParseResult::Complete(RedisItem::Null)),
            b'#' => match self.buffer[1] {
                b't' => Ok(ParseResult::Complete(RedisItem::Boolean(true))),
                b'f' => Ok(ParseResult::Complete(RedisItem::Boolean(false))),
                _ => Err(ParseError::Invalid),
            },
            b'$' => {
                self.buffer.clear();
                let read1 = stream.read_until(b'\n', &mut self.buffer).await?;
                if read1 < 2 {
                    return Err(ParseError::Incomplete);
                }
                let Ok(strval) = std::str::from_utf8(&self.buffer[..read1-2]) else {
                    return Err(ParseError::Invalid)
                };
                Ok(ParseResult::Complete(RedisItem::BulkString(
                    strval.to_string(),
                )))
            }
            x @ (b'-' | b'+' | b':') => {
                self.buffer.clear();
                let read1 = stream.read_until(b'\n', &mut self.buffer).await?;
                if read1 < 2 {
                    return Err(ParseError::Incomplete);
                }
                let Ok(strval) = std::str::from_utf8(&self.buffer[..read1-2]) else {
                    return Err(ParseError::Invalid)
                };
                let str = strval.to_string();
                Ok(ParseResult::Complete(match x {
                    b'+' => RedisItem::SimpleString(str),
                    b'-' => RedisItem::SimpleError(str),
                    b':' => {
                        if let Ok(intval) = str.parse::<i64>() {
                            RedisItem::Integer(intval)
                        } else {
                            return Err(ParseError::Invalid);
                        }
                    }
                    _ => unreachable!(),
                }))
            }
            b'*' => {
                let len = std::str::from_utf8(&self.buffer[1..read0 - 2])
                    .unwrap()
                    .parse::<u32>()
                    .unwrap();
                Ok(ParseResult::Partial(ParseState::List {
                    remaining: len as usize,
                    items: Vec::new(),
                }))
            }
            _ => Err(ParseError::Invalid),
        }
    }

    pub async fn parse(
        &mut self,
        stream: &mut BufReader<&Async<TcpStream>>,
    ) -> Result<RedisItem, ParseError> {
        self.buffer.clear();
        self.stack.clear();

        let res = self.parse_partial(stream).await?;
        match res {
            ParseResult::Complete(item) => {
                return Ok(item);
            }
            ParseResult::Partial(state) => {
                self.stack.push(state);
            }
        }

        while let Some(state) = self.stack.pop() {
            let res = self.parse_partial(stream).await?;
            match (res, state) {
                (ParseResult::Partial(new_state), s) => {
                    self.stack.push(s);
                    self.stack.push(new_state);
                }
                (
                    ParseResult::Complete(value),
                    ParseState::List {
                        remaining,
                        mut items,
                    },
                ) => {
                    items.push(value);
                    if remaining == 1 {
                        return Ok(RedisItem::Array(items));
                    } else {
                        self.stack.push(ParseState::List {
                            remaining: remaining - 1,
                            items,
                        });
                    }
                }
            }
        }
        Err(ParseError::Incomplete)
    }
}