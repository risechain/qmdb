use std::sync::mpsc::{sync_channel, Receiver, RecvError, SendError, SyncSender};

pub struct Producer<T: Clone> {
    fwd_sender: SyncSender<T>,
    bck_receiver: Receiver<T>,
}

pub struct Consumer<T: Clone> {
    bck_sender: SyncSender<T>,
    fwd_receiver: Receiver<T>,
}

pub fn new<T: Clone>(size: usize, t: &T) -> (Producer<T>, Consumer<T>) {
    let (fwd_sender, fwd_receiver) = sync_channel(size);
    let (bck_sender, bck_receiver) = sync_channel(size);
    for _ in 0..size {
        bck_sender.send(t.clone()).unwrap();
    }
    let prod = Producer {
        fwd_sender,
        bck_receiver,
    };
    let cons = Consumer {
        bck_sender,
        fwd_receiver,
    };
    (prod, cons)
}

impl<T: Clone> Producer<T> {
    pub fn produce(&mut self, t: T) -> Result<(), SendError<T>> {
        self.fwd_sender.send(t)
    }
    pub fn receive_returned(&mut self) -> Result<T, RecvError> {
        self.bck_receiver.recv()
    }
}

impl<T: Clone> Consumer<T> {
    pub fn consume(&mut self) -> T {
        self.fwd_receiver.recv().unwrap()
    }
    pub fn send_returned(&mut self, t: T) {
        self.bck_sender.send(t).unwrap();
    }
}
