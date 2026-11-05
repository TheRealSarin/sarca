use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio_util::sync::CancellationToken;

use crate::wire::Frame;

pub struct ByteBudget {
    used: AtomicUsize,
    limit: usize,
}

impl ByteBudget {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            used: AtomicUsize::new(0),
            limit,
        })
    }

    pub fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    fn try_reserve(&self, bytes: usize) -> bool {
        self.used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.limit)
            })
            .is_ok()
    }

    fn release(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::AcqRel);
    }
}

struct Charge {
    session: Arc<ByteBudget>,
    global: Arc<ByteBudget>,
    bytes: usize,
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.session.release(self.bytes);
        self.global.release(self.bytes);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OfferError {
    FramesFull,
    BytesFull,
    Closed,
}

pub(crate) struct OutboundSender {
    tx: mpsc::Sender<(Frame, Charge)>,
    session: Arc<ByteBudget>,
    global: Arc<ByteBudget>,
    closed: CancellationToken,
}

impl OutboundSender {
    pub(crate) fn offer(
        &self,
        bytes: usize,
        build: impl FnOnce() -> Frame,
    ) -> Result<(), OfferError> {
        let permit = self.tx.try_reserve().map_err(|err| match err {
            TrySendError::Full(()) => OfferError::FramesFull,
            TrySendError::Closed(()) => OfferError::Closed,
        })?;
        if !self.session.try_reserve(bytes) {
            return Err(OfferError::BytesFull);
        }
        if !self.global.try_reserve(bytes) {
            self.session.release(bytes);
            return Err(OfferError::BytesFull);
        }
        let charge = Charge {
            session: self.session.clone(),
            global: self.global.clone(),
            bytes,
        };
        permit.send((build(), charge));
        Ok(())
    }

    pub(crate) fn close(&self) {
        self.closed.cancel();
    }
}

pub struct Outbound {
    rx: mpsc::Receiver<(Frame, Charge)>,
    closed: CancellationToken,
}

impl Outbound {
    pub async fn recv(&mut self) -> Option<Frame> {
        self.rx.recv().await.map(|(frame, _charge)| frame)
    }

    pub fn try_recv(&mut self) -> Option<Frame> {
        self.rx.try_recv().ok().map(|(frame, _charge)| frame)
    }

    pub fn closed(&self) -> impl Future<Output = ()> + Send + 'static {
        self.closed.clone().cancelled_owned()
    }

    pub fn is_closed(&self) -> bool {
        self.closed.is_cancelled()
    }
}

pub(crate) fn channel(
    slots: usize,
    session_limit: usize,
    global: Arc<ByteBudget>,
) -> (OutboundSender, Outbound) {
    let (tx, rx) = mpsc::channel(slots.max(1));
    let closed = CancellationToken::new();
    let sender = OutboundSender {
        tx,
        session: ByteBudget::new(session_limit),
        global,
        closed: closed.clone(),
    };
    (sender, Outbound { rx, closed })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ping() -> Frame {
        Frame::Ping { nonce: 0 }
    }

    #[test]
    fn byte_budgets_are_enforced_and_released() {
        let global = ByteBudget::new(250);
        let (first, mut first_rx) = channel(8, 200, global.clone());
        let (second, second_rx) = channel(8, 200, global.clone());

        assert_eq!(first.offer(150, ping), Ok(()));
        assert_eq!(first.offer(100, ping), Err(OfferError::BytesFull));
        assert_eq!(second.offer(100, ping), Ok(()));
        assert_eq!(second.offer(50, ping), Err(OfferError::BytesFull));
        assert_eq!(global.used(), 250);

        assert!(first_rx.try_recv().is_some());
        assert_eq!(global.used(), 100);
        drop(second_rx);
        assert_eq!(global.used(), 0);
        assert_eq!(second.offer(1, ping), Err(OfferError::Closed));
    }

    #[test]
    fn frame_slots_are_enforced_without_building_the_frame() {
        let (sender, _rx) = channel(1, 1_000, ByteBudget::new(1_000));
        assert_eq!(sender.offer(1, ping), Ok(()));
        let result = sender.offer(1, || panic!("frame built despite a full queue"));
        assert_eq!(result, Err(OfferError::FramesFull));
    }

    #[test]
    fn close_signals_the_receiver() {
        let (sender, rx) = channel(1, 1, ByteBudget::new(1));
        assert!(!rx.is_closed());
        sender.close();
        assert!(rx.is_closed());
    }
}
