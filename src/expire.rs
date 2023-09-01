use std::{
    cell::RefCell,
    collections::BinaryHeap,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Instant,
};

use smol::{stream::Stream, Timer};

use crate::State;

#[derive(Debug)]
pub struct Expire {
    items: BinaryHeap<Expiry>,
    pub waker: Option<Waker>,
}

impl Expire {
    pub fn new() -> Self {
        Self {
            items: BinaryHeap::new(),
            waker: None,
        }
    }

    pub fn push(&mut self, key: String, time: Instant) {
        self.items.push(Expiry { key, time });
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

#[derive(Debug)]
pub enum ExpireResult {
    Expired(String),
    Updated,
    Check,
}

pub fn expire(state: &RefCell<State>) -> impl Future<Output = ExpireResult> + '_ {
    ExpireFuture { expire: state }
}

pub fn retry(state: &RefCell<State>) -> impl Future<Output = ExpireResult> {
    let state = state.borrow();
    let timer = state.expire.items.peek().map_or_else(
        || Timer::never(),
        |e| Timer::at(e.time),
    );
    async {
        timer.await;
        ExpireResult::Check
    }
}

pub fn check(state: &RefCell<State>) -> impl Future<Output = ExpireResult> + '_ {
    CheckFuture { expire: state }
}

struct ExpireFuture<'a> {
    expire: &'a RefCell<State>,
}

impl<'a> Future for ExpireFuture<'a> {
    type Output = ExpireResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        println!("poll");

        let this = self.get_mut();

        let mut state = this.expire.borrow_mut();
        if let Some(head_expire) = state.expire.items.peek().map(|e| e.time.clone()) {
            if head_expire <= Instant::now() {
                let head = state.expire.items.pop().unwrap();
                return Poll::Ready(ExpireResult::Expired(head.key));
            }
        }
        // if state
        //     .expire
        //     .waker
        //     .as_ref()
        //     .map(|w| !w.will_wake(cx.waker()))
        //     .unwrap_or(true)
        // {
        //     state.expire.waker = Some(cx.waker().clone());
        // }
        state.expire.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

struct CheckFuture<'a> {
    expire: &'a RefCell<State>,
}

impl<'a> Future for CheckFuture<'a> {
    type Output = ExpireResult;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        let mut state = this.expire.borrow_mut();
        if !state.expire.items.is_empty() {
            Poll::Ready(ExpireResult::Updated)
        } else {
            state.expire.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[derive(Debug)]
pub struct Expiry {
    pub key: String,
    pub time: Instant,
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
