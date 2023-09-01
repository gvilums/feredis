pub mod expire;
pub mod item;

use std::collections::{HashMap, VecDeque};

use std::cell::RefCell;
use std::net::{TcpListener, TcpStream};
use std::time::Instant;

use smol::io::{AsyncWriteExt, BufReader};
use smol::Async;
use std::io;


use expire::Expire;
use item::RedisItem;

#[derive(Debug)]
pub struct State {
    stop: bool,
    items: HashMap<String, (RedisItem, u64)>,
    expire: Expire,
    counter: u64,
}

struct ItemStore {
    // keys: HashMap<String, usize>,
    // items: HashMap<usize, RedisItem>,
    items: HashMap<String, (RedisItem, u64)>,
    counter: u64,
}

impl State {
    fn new() -> Self {
        Self {
            stop: false,
            items: HashMap::new(),
            expire: Expire::new(),
            counter: 0,
        }
    }
}


#[derive(Debug)]
enum RedisError {
    InvalidCommand,
    InvalidArguments,
    WrongType,
    UnknownCommand,
}

impl From<RedisError> for RedisItem {
    fn from(value: RedisError) -> Self {
        use RedisError::*;
        use RedisItem::SimpleError;
        match value {
            InvalidCommand => SimpleError("invalid command".to_string()),
            InvalidArguments => SimpleError("invalid arguments".to_string()),
            WrongType => SimpleError("WRONGTYPE".to_string()),
            UnknownCommand => SimpleError("unknown command".to_string()),
        }
    }
}

fn do_ping(mut args: VecDeque<RedisItem>, _: &RefCell<State>) -> RedisItem {
    use RedisItem::*;
    if let Some(BulkString(val)) = args.pop_front() {
        RedisItem::BulkString(val)
    } else {
        RedisItem::SimpleString("PONG".to_string())
    }
}

fn do_set(mut args: VecDeque<RedisItem>, state: &RefCell<State>) -> RedisItem {
    use RedisItem::*;
    let Some(BulkString(key)) = args.pop_front() else {
        return SimpleError("invalid arguments".to_string());
    };
    let Some(val @ BulkString(_)) = args.pop_front() else {
        return SimpleError("invalid arguments".to_string());
    };
    let mut state = state.borrow_mut();
    let id = state.counter;
    state.items.insert(key, (val, id));
    state.counter += 1;
    SimpleString("OK".to_string())
}

fn do_get(mut args: VecDeque<RedisItem>, state: &RefCell<State>) -> RedisItem {
    use RedisItem::*;
    let Some(BulkString(key)) = args.pop_front() else {
        return SimpleError("invalid arguments".to_string());
    };
    match state.borrow().items.get(&key).map(|(val, _)| val) {
        Some(BulkString(val)) => BulkString(val.clone()),
        Some(_) => SimpleError("value is not a string".to_string()),
        None => Null,
    }
}

fn do_del(mut args: VecDeque<RedisItem>, state: &RefCell<State>) -> RedisItem {
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

fn do_expire(mut args: VecDeque<RedisItem>, state: &RefCell<State>) -> RedisItem {
    use RedisItem::*;
    let Some(BulkString(key)) = args.pop_front() else {
        return SimpleError("invalid arguments".to_string());
    };
    let Some(BulkString(val)) = args.pop_front() else {
        return SimpleError("invalid arguments".to_string());
    };
    let Ok(time) = val.parse::<u64>() else {
        return SimpleError("invalid arguments".to_string());
    };
    let Some(id) = state.borrow().items.get(&key).map(|(_, id)| *id) else {
        return Integer(0)
    };
    let time = Instant::now() + std::time::Duration::from_secs(time);
    state.borrow_mut().expire.push(key, id, time);
    Integer(1)
}

fn do_rename(mut args: VecDeque<RedisItem>, state: &RefCell<State>) -> RedisItem {
    use RedisItem::*;
    let Some(BulkString(key)) = args.pop_front() else {
        return SimpleError("invalid arguments".to_string());
    };
    let Some(BulkString(new_key)) = args.pop_front() else {
        return SimpleError("invalid arguments".to_string());
    };
    let mut state = state.borrow_mut();
    if let Some((val, id)) = state.items.remove(&key) {
        if let Some(exp) = state.expire.get_expiry(id) {
            state.expire.push(new_key.clone(), id, exp);
        }
        state.items.insert(new_key, (val, id));
        SimpleString("OK".to_string())
    } else {
        SimpleError("no such key".to_string())
    }
}

fn do_rpush(mut args: VecDeque<RedisItem>, state: &RefCell<State>) -> RedisItem {
    use RedisItem::*;
    let Some(BulkString(key)) = args.pop_front() else {
        return SimpleError("invalid arguments".to_string());
    };
    let mut state = state.borrow_mut();
    let id = state.counter;
    state.counter += 1;
    let entry = state.items.entry(key).or_insert_with(|| (Array(Vec::new()), id));
    let (Array(items), _) = entry else {
        return SimpleError("WRONGTYPE".to_string());
    };
    while let Some(item) = args.pop_front() {
        items.push(item);
    }
    Integer(items.len() as i64)
}

fn do_rpop(mut args: VecDeque<RedisItem>, state: &RefCell<State>) -> RedisItem {
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
    let Some((Array(items), _)) = state.items.get_mut(&key) else {
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
                "ping" => do_ping,
                "set" => do_set,
                "get" => do_get,
                "del" => do_del,
                "expire" => do_expire,
                "rename" => do_rename,
                "rpush" => do_rpush,
                "rpop" => do_rpop,
                _ => return SimpleError("unknown command".to_string()),
            };
            handler(args, state)
        }
        _ => SimpleError("unknown command".to_string()),
    }
}

async fn connection_worker(stream: Async<TcpStream>, state: &RefCell<State>) -> io::Result<()> {
    use item::{ParseError, ItemParser};
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
                let res = handle_command(command, state);
                out_buffer.clear();
                res.serialize(&mut out_buffer);
                writer.write_all(&out_buffer[..]).await?;
            }
        }
    }
}

fn main() -> io::Result<()> {
    let state = RefCell::new(State::new());
    let exec = smol::LocalExecutor::new();
    exec.spawn(expire::expire_worker(&state)).detach();
    smol::block_on(exec.run(async {
        // Create a listener.
        let listener = Async::<TcpListener>::bind(([127, 0, 0, 1], 7000))?;
        println!("Listening on {}", listener.get_ref().local_addr()?);

        // Accept clients in a loop.
        loop {
            let (stream, peer_addr) = listener.accept().await?;
            println!("Accepted client: {}", peer_addr);
            exec.spawn(connection_worker(stream, &state)).detach();
        }
    }))
}
