use std::io;

use smol::io::{AsyncBufRead, AsyncBufReadExt};

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

#[derive(Debug)]
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

#[derive(Debug)]
enum ParseResult {
    Partial(ParseState),
    Complete(RedisItem),
}

pub struct ItemParser {
    buffer: Vec<u8>,
    stack: Vec<ParseState>,
}

#[derive(Debug)]
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
        stream: &mut (impl AsyncBufRead + Unpin),
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
                let Ok(strval) = std::str::from_utf8(&self.buffer[1..read0-2]) else {
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
                    .map_err(|_| ParseError::Invalid)?
                    .parse::<u32>()
                    .map_err(|_| ParseError::Invalid)?;
                Ok(ParseResult::Partial(ParseState::List {
                    remaining: len as usize,
                    items: Vec::new(),
                }))
            }
            _ => Err(ParseError::Invalid),
        }
    }

    pub async fn parse<T>(&mut self, stream: &mut T) -> Result<RedisItem, ParseError>
    where
        T: AsyncBufRead + Unpin,
    {
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

        while let Some(mut state) = self.stack.pop() {
            let res = if let ParseState::List {
                remaining: 0,
                items,
            } = state
            {
                let res = RedisItem::Array(items);
                if let Some(newstate) = self.stack.pop() {
                    state = newstate;
                } else {
                    return Ok(res);
                }
                ParseResult::Complete(res)
            } else {
                self.parse_partial(stream).await?
            };
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
                    if remaining == 0 {
                        return Err(ParseError::Invalid);
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

#[cfg(test)]
mod test {
    use super::*;

    fn parse(input: &[u8]) -> Result<RedisItem, ParseError> {
        let mut parser = ItemParser::new();
        let mut stream = smol::io::Cursor::new(input);
        smol::block_on(parser.parse(&mut stream))
    }

    #[test]
    pub fn test_parse_simple() {
        let res = parse(b"+OK\r\n").unwrap();
        assert_eq!(res, RedisItem::SimpleString("OK".to_string()));
    }

    #[test]
    pub fn test_parse_error() {
        let res = parse(b"-MYERROR\r\n").unwrap();
        assert_eq!(res, RedisItem::SimpleError("MYERROR".to_string()));
    }

    #[test]
    pub fn test_parse_integer() {
        let res = parse(b":12345\r\n").unwrap();
        assert_eq!(res, RedisItem::Integer(12345));
    }

    #[test]
    pub fn test_parse_bulk_string() {
        let res = parse(b"$6\r\nfoobar\r\n").unwrap();
        assert_eq!(res, RedisItem::BulkString("foobar".to_string()));
    }

    #[test]
    pub fn test_parse_array() {
        let res = parse(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").unwrap();
        assert_eq!(
            res,
            RedisItem::Array(vec![
                RedisItem::BulkString("foo".to_string()),
                RedisItem::BulkString("bar".to_string())
            ])
        );
    }

    #[test]
    pub fn test_parse_nested_array() {
        let res = parse(b"*2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n").unwrap();
        assert_eq!(
            res,
            RedisItem::Array(vec![
                RedisItem::Array(vec![
                    RedisItem::BulkString("foo".to_string()),
                    RedisItem::BulkString("bar".to_string())
                ]),
                RedisItem::BulkString("baz".to_string())
            ])
        );
    }

    #[test]
    pub fn test_parse_null() {
        let res = parse(b"_\r\n").unwrap();
        assert_eq!(res, RedisItem::Null);
    }

    #[test]
    pub fn test_parse_boolean() {
        let res_true = parse(b"#t\r\n").unwrap();
        assert_eq!(res_true, RedisItem::Boolean(true));

        let res_false = parse(b"#f\r\n").unwrap();
        assert_eq!(res_false, RedisItem::Boolean(false));
    }
}
