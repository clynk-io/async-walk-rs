use std::path::PathBuf;

#[derive(Debug)]
pub enum Error {
    SymlinkCycle(PathBuf),
    Io(PathBuf, ::std::io::Error),
    Filter(PathBuf, Box<dyn ::std::error::Error + Send>),
}

pub type Result<T> = ::std::result::Result<T, Error>;

impl Error {
    pub fn symlink_cycle(self) -> Option<PathBuf> {
        if let Error::SymlinkCycle(p) = self {
            Some(p)
        } else {
            None
        }
    }

    pub fn io(self) -> Option<(PathBuf, ::std::io::Error)> {
        if let Error::Io(p, e) = self {
            Some((p, e))
        } else {
            None
        }
    }

    pub fn filter(self) -> Option<(PathBuf, Box<dyn ::std::error::Error + Send>)> {
        if let Error::Filter(p, e) = self {
            Some((p, e))
        } else {
            None
        }
    }
}
