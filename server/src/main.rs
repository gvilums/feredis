pub mod expire;

use std::collections::{HashMap, VecDeque};

use std::cell::RefCell;
use std::net::{TcpListener, TcpStream};
use std::time::Instant;

use smol::io::{AsyncWriteExt, BufReader};
use smol::Async;
use std::io;

use expire::Expire;
use feredis_core::item::RedisItem;

#[derive(Debug)]
pub struct State {
    stop: bool,
    items: HashMap<String, (RedisItem, u64)>,
    expire: Expire,
    tag_counter: u64,
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
            tag_counter: 0,
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
    let tag = state.tag_counter;
    state.items.insert(key, (val, tag));
    state.tag_counter += 1;
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
    let Some(tag) = state.borrow().items.get(&key).map(|(_, tag)| *tag) else {
        return Integer(0)
    };
    let time = Instant::now() + std::time::Duration::from_secs(time);
    let mut state = state.borrow_mut();
    let state = &mut *state;
    if let Some(_) = state.expire.get_expiry(tag) {
        let (_, tag_mut) = state.items.get_mut(&key).unwrap();
        *tag_mut = state.tag_counter;
        state.tag_counter += 1;
        state.expire.push(key, *tag_mut, time);
    } else {
        state.expire.push(key, tag, time);
    }
    Integer(1)
}

fn do_persist(mut args: VecDeque<RedisItem>, state: &RefCell<State>) -> RedisItem {
    use RedisItem::*;
    let Some(BulkString(key)) = args.pop_front() else {
        return SimpleError("invalid arguments".to_string());
    };
    let mut state = state.borrow_mut();
    let state = &mut *state;
    // by updating the tag we give the item a new "identity",
    // preventing it from being expired
    if let Some((_, tag)) = state.items.get_mut(&key) {
        *tag = state.tag_counter;
        state.tag_counter += 1;
        Integer(1)
    } else {
        Integer(0)
    }
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
    if let Some((val, tag)) = state.items.remove(&key) {
        if let Some(exp) = state.expire.get_expiry(tag) {
            state.expire.push(new_key.clone(), tag, exp);
        }
        state.items.insert(new_key, (val, tag));
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
    let tag = state.tag_counter;
    state.tag_counter += 1;
    let entry = state
        .items
        .entry(key)
        .or_insert_with(|| (Array(Vec::new()), tag));
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
                "persist" => do_persist,
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
    use feredis_core::item::{ItemParser, ParseError};
    let mut reader = BufReader::new(&stream);
    let mut writer = &stream;

    let mut parser = ItemParser::new();
    let mut out_buffer = Vec::new();
    loop {
        let res = match parser.parse(&mut reader).await {
            Ok(command) => handle_command(command, state),
            Err(ParseError::Incomplete | ParseError::Invalid) => {
                RedisItem::SimpleError("ERR".to_string())
            }
            Err(ParseError::IoError(err)) => return Err(err),
        };
        out_buffer.clear();
        res.serialize(&mut out_buffer);
        writer.write_all(&out_buffer[..]).await?;
    }
}

fn main() -> io::Result<()> {
    let port = std::env::var("PORT")
        .map_or(Ok(7000), |s| s.parse::<u16>())
        .expect("port must be a number");


    let state = RefCell::new(State::new());
    let exec = smol::LocalExecutor::new();
    exec.spawn(expire::expire_worker(&state)).detach();
    smol::block_on(exec.run(async {
        // Create a listener.
        let listener = Async::<TcpListener>::bind(([127, 0, 0, 1], port))?;
        println!("Listening on {}", listener.get_ref().local_addr()?);

        // Accept clients in a loop.
        loop {
            let (stream, peer_addr) = listener.accept().await?;
            println!("Accepted client: {}", peer_addr);
            exec.spawn(connection_worker(stream, &state)).detach();
        }
    }))
}
