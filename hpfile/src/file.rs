use std::{
    io::{self, SeekFrom},
    path::Path,
};

use anyhow::Result;
use parking_lot::RwLock;

pub struct Options {}

impl Options {
    pub fn read(&self, _: bool) -> &Self {
        self
    }

    pub fn write(&self, _: bool) -> &Self {
        self
    }

    pub fn custom_flags(&self, _: i32) -> &Self {
        self
    }

    pub fn create(&self, _: bool) -> &Self {
        self
    }

    pub fn open<P: AsRef<Path>>(&self, _: P) -> Result<File, io::Error> {
        Ok(File::new())
    }
}

pub struct Metadata {
    len: usize,
}
impl Metadata {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[derive(Debug)]
pub struct File {
    data: RwLock<Vec<u8>>,
}

impl File {
    fn new() -> Self {
        File {
            data: RwLock::new(Vec::new()),
        }
    }

    pub fn open<P: AsRef<Path>>(_: P) -> Result<File, io::Error> {
        Ok(File::new())
    }

    pub fn options() -> Options {
        Options {}
    }

    pub fn metadata(&self) -> Result<Metadata> {
        Ok(Metadata {
            len: self.data.read().len(),
        })
    }

    pub fn create_new<P: AsRef<Path>>(_: P) -> Result<File> {
        Ok(File::new())
    }

    pub fn set_len(&self, len: u64) -> Result<(), io::Error> {
        self.data.write().truncate(len as usize);
        Ok(())
    }

    pub fn seek(&self, _: SeekFrom) -> Result<(), io::Error> {
        Ok(())
    }

    pub fn write(&self, buffer: &[u8]) -> Result<usize, io::Error> {
        self.data.write().extend_from_slice(buffer);
        Ok(0)
    }

    pub fn sync_all(&self) -> Result<(), io::Error> {
        Ok(())
    }

    pub fn read(&self, bz: &mut [u8]) -> Result<usize, io::Error> {
        let data = self.data.read();
        let len = data.len();
        let read_len = usize::min(bz.len(), len);

        let s = &data[..read_len];
        bz[..read_len].copy_from_slice(s);

        Ok(read_len)
    }

    pub fn read_at(&self, bz: &mut [u8], offset: u64) -> Result<usize, io::Error> {
        let data = self.data.read();
        let len = data.len() as u64;
        if offset > data.len() as u64 {
            return Ok(0);
        }

        let read_len = usize::min(bz.len(), (len - offset) as usize);
        let s = &data[offset as usize..offset as usize + read_len];
        bz[..read_len].copy_from_slice(s);

        Ok(read_len)
    }
}
