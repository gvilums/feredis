pub mod blocking;

use std::collections::{HashMap, VecDeque};

use std::cell::RefCell;
use std::net::{TcpListener, TcpStream};

use smol::io::{*, AsyncBufReadExt, AsyncWriteExt, BufReader};
use smol::{Async, Timer};

struct State {
    items: HashMap<String, RedisItem>,
}

impl State {
    fn new() -> Self {
        Self {
            items: HashMap::new(),
        }
    }
}

// enum NewItem<'a> {
//     SimpleString(&'a str),
//     SimpleError(&'a str),
//     Integer(i64),
//     BulkString(&'a str),
//     Array(Vec<NewItem<'a>>),
//     Null,
//     Boolean(bool),
//     Double(f64),
// }

// enum RawItem {
//     SimpleString(usize, usize),
//     SimpleError(usize, usize),
//     Integer(usize, usize),
//     BulkString(usize, usize),
//     Array(usize, usize),
//     Null,
//     Boolean(bool),
//     Double(usize, usize),
// }

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum RedisItem {
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
    fn serialize(&self, target: &mut Vec<u8>) {
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
    // #[async_recursion::async_recursion(?Send)]
    // async fn from_stream(stream: &mut BufReader<&Async<TcpStream>>, buffer: &mut Vec<u8>) -> io::Result<Self> {
    //     buffer.clear();
    //     let count = stream.read_until(b'\n', buffer).await?;
    //     assert!(count >= 3); // at least _\r\n
    //     match buffer[0] {
    //         b'$' => {
    //             // let len = std::str::from_utf8(&buffer[1..count-2]).unwrap().parse::<u32>().unwrap();
    //             buffer.clear();
    //             let count = stream.read_until(b'\n', buffer).await?;
    //             return Ok(Self::BulkString(std::str::from_utf8(&buffer[..count-2]).unwrap().to_string()))
    //         },
    //         b'*' => {
    //             let len = std::str::from_utf8(&buffer[1..count-2]).unwrap().parse::<u32>().unwrap();
    //             let mut items = Vec::new();
    //             for _ in 0..len {
    //                 items.push(Self::from_stream(stream, buffer).await?);
    //             }
    //             return Ok(Self::List(items))
    //         },
    //         _ => {
    //             panic!("unknown command");
    //         }
    //     }
    // }
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

struct ItemParser {
    buffer: Vec<u8>,
    stack: Vec<ParseState>,
}

enum ParseError {
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
    fn new() -> Self {
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

    async fn parse(
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

fn handle_ping(_: VecDeque<RedisItem>, _: &RefCell<State>) -> RedisItem {
    RedisItem::SimpleString("PONG".to_string())
}

fn handle_set(mut args: VecDeque<RedisItem>, state: &RefCell<State>) -> RedisItem {
    use RedisItem::*;
    let Some(BulkString(key)) = args.pop_front() else {
        return SimpleError("invalid arguments".to_string());
    };
    let Some(val @ BulkString(_)) = args.pop_front() else {
        return SimpleError("invalid arguments".to_string());
    };
    state.borrow_mut().items.insert(key, val);
    SimpleString("OK".to_string())
}

fn handle_get(mut args: VecDeque<RedisItem>, state: &RefCell<State>) -> RedisItem {
    use RedisItem::*;
    let Some(BulkString(key)) = args.pop_front() else {
        return SimpleError("invalid arguments".to_string());
    };
    match state.borrow().items.get(&key) {
        Some(BulkString(val)) => BulkString(val.clone()),
        Some(_) => SimpleError("value is not a string".to_string()),
        None => Null,
    }
}

fn handle_del(mut args: VecDeque<RedisItem>, state: &RefCell<State>) -> RedisItem {
    use RedisItem::*;
    let mut counter = 0;
    while let Some(item) = args.pop_front() {
        let BulkString(key) = item else {
            return SimpleError("invalid arguments".to_string());
        };
        if let Some(_) = state.borrow_mut().items.remove(&key) {
            counter += 1;
        }
    }
    Integer(counter)
}

fn handle_rpush(mut args: VecDeque<RedisItem>, state: &RefCell<State>) -> RedisItem {
    use RedisItem::*;
    let Some(BulkString(key)) = args.pop_front() else {
        return SimpleError("invalid arguments".to_string());
    };
    let mut state = state.borrow_mut();
    let entry = state.items.entry(key).or_insert_with(|| Array(Vec::new()));
    let Array(items) = entry else {
        return SimpleError("WRONGTYPE".to_string());
    };
    while let Some(item) = args.pop_front() {
        items.push(item);
    }
    Integer(items.len() as i64)
}

fn handle_rpop(mut args: VecDeque<RedisItem>, state: &RefCell<State>) -> RedisItem {
    enum PopCount {
        Single,
        Count(usize),
    }
    use RedisItem::*;
    let Some(BulkString(key)) = args.pop_front() else {
        return SimpleError("invalid arguments".to_string());
    };
    let count = match args.pop_front() {
        Some(Integer(val)) => {
            if val < 0 {
                return SimpleError("invalid arguments".to_string());
            }
            PopCount::Count(val as usize)
        }
        Some(BulkString(v) | SimpleString(v)) => {
            let Ok(val) = v.parse::<usize>() else {
                return SimpleError("invalid arguments".to_string());
            };
            PopCount::Count(val)
        }
        None => PopCount::Single,
        _ => return SimpleError("invalid arguments".to_string()),
    };
    let mut state = state.borrow_mut();
    let Some(Array(items)) = state.items.get_mut(&key) else {
        return Null;
    };
    // empty lists should not exist
    assert!(items.len() > 0);
    let res = match count {
        PopCount::Single => items.pop().unwrap(),
        PopCount::Count(n) => {
            let mut res = Vec::new();
            for _ in 0..n {
                if let Some(item) = items.pop() {
                    res.push(item);
                } else {
                    break;
                }
            }
            Array(res)
        }
    };
    if items.is_empty() {
        state.items.remove(&key);
    }
    res
}

fn handle_command(command: RedisItem, state: &RefCell<State>) -> RedisItem {
    use RedisItem::*;
    match command {
        Array(items) => {
            let mut args = VecDeque::from(items);
            let Some(BulkString(mut command) | SimpleString(mut command)) = args.pop_front() else {
                return SimpleError("invalid command".to_string());
            };
            command.make_ascii_lowercase();
            let handler = match command.as_str() {
                "ping" => handle_ping,
                "set" => handle_set,
                "get" => handle_get,
                "del" => handle_del,
                "rpush" => handle_rpush,
                "rpop" => handle_rpop,
                _ => return SimpleError("unknown command".to_string()),
            };
            handler(args, state)
        }
        _ => SimpleError("unknown command".to_string()),
    }
}

/// Echoes messages from the client back to it.
async fn handle_connection(stream: Async<TcpStream>, state: &RefCell<State>) -> io::Result<()> {
    let mut reader = BufReader::new(&stream);
    let mut writer = &stream;

    let mut parser = ItemParser::new();
    let mut out_buffer = Vec::new();
    loop {
        match parser.parse(&mut reader).await {
            Err(ParseError::Incomplete | ParseError::Invalid) => {
                writer.write_all(b"-ERR\r\n").await?;
                continue;
            }
            Err(ParseError::IoError(err)) => return Err(err),
            Ok(command) => {
                dbg!(&command);
                let res = handle_command(command, state);
                out_buffer.clear();
                res.serialize(&mut out_buffer);
                writer.write_all(&out_buffer[..]).await?;
            }
        }
    }
    // Ok(())
}

async fn handle_expire(state: &RefCell<State>) {

}

fn main() -> io::Result<()> {
    let state = RefCell::new(State::new());
    let exec = smol::LocalExecutor::new();
    // blocking::main()
    smol::block_on(exec.run(async {
        // Create a listener.
        let listener = Async::<TcpListener>::bind(([127, 0, 0, 1], 7000))?;
        println!("Listening on {}", listener.get_ref().local_addr()?);
        println!("Now start a TCP client.");

        // Accept clients in a loop.
        loop {
            let (stream, peer_addr) = listener.accept().await?;
            println!("Accepted client: {}", peer_addr);
            exec.spawn(handle_connection(stream, &state)).detach();
        }
    }))
    // println!("counter: {}", counter.borrow());
    // Ok(())
}
