use std::{
    cell::RefCell,
    collections::BinaryHeap,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Instant, cmp::Reverse,
};

use smol::Timer;

use crate::State;

pub async fn expire_worker(state: &RefCell<State>) {
    use smol::future::or;
    loop {
        or(until_expire(state), until_update(state)).await;
        println!("Expire worker wakeup");
        let mut state = state.borrow_mut();

        if let Some(exp) = state.expire.items.peek().map(|e| e.0.time) {
            if exp <= Instant::now() {
                let exp = state.expire.items.pop().unwrap().0;
                if let Some(id) = state.items.get(&exp.key).map(|it| it.1) {
                    if id == exp.id {
                        println!("Expired: {}", &exp.key);
                        state.items.remove(&exp.key);
                    } else {
                        println!("Skipping: {} (not latest)", &exp.key);
                    }
                }
            }
        }

        if state.stop {
            break;
        }
    }
}

#[derive(Debug)]
pub struct Expire {
    items: BinaryHeap<Reverse<Expiry>>,
    waker: Option<Waker>,
    updated: bool,
}

impl Expire {
    pub fn new() -> Self {
        Self {
            items: BinaryHeap::new(),
            waker: None,
            updated: false,
        }
    }

    pub fn push(&mut self, key: String, id: u64, time: Instant) {
        // get the previously closest expiry time
        let prev_exp = self.items.peek().map(|e| e.0.time);
        // add new expire for key
        self.items.push(Reverse(Expiry { key, time, id }));
        // if the new expiry time is closer than the previous one, wake the worker
        if prev_exp.map(|e| e > time).unwrap_or(true) {
            self.updated = true;
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }
}

fn until_expire(state: &RefCell<State>) -> impl Future<Output = ()> + '_ {
    let timer = state
        .borrow()
        .expire
        .items
        .peek()
        .map_or_else(|| Timer::never(), |e| Timer::at(e.0.time));
    async {
        timer.await;
    }
}

fn until_update(state: &RefCell<State>) -> impl Future<Output = ()> + '_ {
    UpdateFuture { expire: state }
}

struct UpdateFuture<'a> {
    expire: &'a RefCell<State>,
}

impl<'a> Future for UpdateFuture<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut state = this.expire.borrow_mut();
        if state.expire.updated {
            state.expire.updated = false;
            Poll::Ready(())
        } else {
            if state
                .expire
                .waker
                .as_ref()
                .map(|w| !w.will_wake(cx.waker()))
                .unwrap_or(true)
            {
                state.expire.waker = Some(cx.waker().clone());
            }
            Poll::Pending
        }
    }
}

#[derive(Debug)]
struct Expiry {
    key: String,
    id: u64,
    time: Instant,
}

impl Ord for Expiry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.time.cmp(&other.time)
    }
}

impl PartialOrd for Expiry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Expiry {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time
    }
}

impl Eq for Expiry {}
