use crate::error::{Error, Result};
use crossbeam_channel::{unbounded, Receiver, Sender};
use futures::future::BoxFuture;
use futures::stream::{Stream, StreamExt};
use std::collections::HashSet;
use std::fs::Metadata;

#[cfg(not(target_os = "windows"))]
use std::os::unix::fs::MetadataExt;
#[cfg(target_os = "windows")]
use std::os::windows::fs::MetadataExt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use std::task::{Context, Poll};
use tokio::fs::{metadata, read_dir, DirEntry};
use tokio::spawn;

pub type FilterResult = ::std::result::Result<bool, Box<dyn ::std::error::Error + Send + Sync>>;

// TODO: Explore making this type cleaner
pub type Filter = Box<dyn Fn(&DirEntry) -> BoxFuture<FilterResult> + Sync + Send>;

// use type alias for readability, represents an ino on linux or file index on windows
type PathId = u64;

enum Entry {
    File(DirEntry),
    Dir(DirEntry, PathId, u64),
    Symlink(DirEntry, PathId, u64),
    Root(Metadata, PathId),
}

pub struct Walk {
    queue: Vec<(PathBuf, u64)>,
    ready_entries: Vec<Result<DirEntry>>,
    receiver: Receiver<Result<Entry>>,
    sender: Sender<Result<Entry>>,
    follow_symlinks: bool,
    counter: Arc<AtomicUsize>,
    concurrency_limit: Option<usize>,
    visited: Option<HashSet<u64>>,
    max_depth: Option<u64>,
    filter: Option<Arc<Filter>>,
}

impl Walk {
    pub fn new(
        root: PathBuf,
        follow_symlinks: bool,
        concurrency_limit: Option<usize>,
        max_level: Option<u64>,
        filter: Option<Filter>,
    ) -> Self {
        let (tx, rx) = unbounded();
        let visited = match follow_symlinks {
            true => Some(HashSet::new()),
            false => None,
        };
        Walk {
            queue: vec![(root, 0)],
            ready_entries: vec![],
            receiver: rx,
            sender: tx,
            follow_symlinks: follow_symlinks,
            counter: Arc::new(AtomicUsize::new(0)),
            concurrency_limit: concurrency_limit,
            visited: visited,
            max_depth: max_level,
            filter: filter.map(|f| Arc::new(f)),
        }
    }
}

fn unique_id(info: &Metadata) -> u64 {
    // Called with fs::metadata so should never be None
    #[cfg(target_os = "windows")]
    let id = info.file_index().unwrap();

    #[cfg(not(target_os = "windows"))]
    let id = info.ino();
    id
}

async fn handle_entry(
    entry: Result<DirEntry>,
    follow_symlinks: bool,
    depth: u64,
    filter: Option<Arc<Filter>>,
) -> Result<Option<Entry>> {
    let entry = entry?;
    if let Some(filter) = filter {
        let include = filter(&entry)
            .await
            .map_err(|e| Error::Filter(entry.path(), e))?;
        if !include {
            return Ok(None);
        }
    }
    let file_type = entry
        .file_type()
        .await
        .map_err(|e| Error::Io(entry.path(), e))?;
    if file_type.is_dir() {
        let unique_id = if follow_symlinks {
            #[cfg(not(target_os = "windows"))]
            let info = entry
                .metadata()
                .await
                .map_err(|e| Error::Io(entry.path(), e))?;

            // we can't use entry.metadata() on windows since it doesn't include the file index when called from DirEntry
            #[cfg(target_os = "windows")]
            let info = metadata(entry.path())
                .await
                .map_err(|e| Error::Io(entry.path(), e))?;

            unique_id(&info)
        } else {
            0 // pass 0 since this will never be used
        };
        Ok(Some(Entry::Dir(entry, unique_id, depth)))
    } else if file_type.is_symlink() && follow_symlinks {
        // follow the symlink to get its type
        let info = metadata(entry.path())
            .await
            .map_err(|e| Error::Io(entry.path(), e))?;
        if info.is_dir() {
            Ok(Some(Entry::Symlink(entry, unique_id(&info), depth)))
        } else {
            Ok(Some(Entry::File(entry)))
        }
    } else {
        Ok(Some(Entry::File(entry)))
    }
}

impl Stream for Walk {
    type Item = Result<DirEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let walk = self.get_mut();
        while !walk.queue.is_empty() {
            // Check how many tasks are currently executing break if we have are at the limit
            let counter = walk.counter.clone();
            if let Some(limit) = walk.concurrency_limit {
                if counter.load(Ordering::Relaxed) == limit {
                    break;
                }
            }
            // Guaranteed to not be none since we check if the queue is empty in the loop
            let (p, depth) = walk.queue.pop().unwrap();
            let sender = walk.sender.clone();
            let filter = walk.filter.clone();
            let follow_symlinks = walk.follow_symlinks;
            // TODO: Double check ordering is what we want
            counter.fetch_add(1, Ordering::Relaxed);
            let waker = cx.waker().clone();
            spawn(async move {
                // TODO: If we add a include_root_dir option, remove follow_symlinks condition
                // We have a special case for the root directory so we store it for symlink cycles
                if depth == 0 && follow_symlinks {
                    // use fs::metadata so we follow the symlink
                    match metadata(&p).await {
                        Ok(info) => {
                            let id = unique_id(&info);
                            let _ = sender.send(Ok(Entry::Root(info, id)));
                        }
                        Err(e) => {
                            let _ = sender.send(Err(Error::Io(p.clone(), e)));
                        }
                    }
                }
                match read_dir(&p).await {
                    Ok(entries) => {
                        entries
                            .map(|res| res.map_err(|e| Error::Io(p.clone(), e)))
                            .for_each(|entry| async {
                                let sender = sender.clone();
                                let waker = waker.clone();
                                let filter = filter.clone();
                                match handle_entry(entry, follow_symlinks, depth + 1, filter).await
                                {
                                    Ok(entry) => {
                                        if let Some(entry) = entry {
                                            let _ = sender.send(Ok(entry));
                                        }
                                    }
                                    Err(e) => {
                                        let _ = sender.send(Err(e));
                                    }
                                };
                                // Wake each time we send, since a result will be ready
                                waker.wake();
                            })
                            .await;
                    }
                    Err(e) => {
                        let _ = sender.send(Err(Error::Io(p, e)));
                    }
                };
                // decrement counter since this task has finished
                counter.fetch_sub(1, Ordering::Relaxed);
                // Wake after decrementing counter since we might have been at the concurrency limit
                waker.wake();
            });
        }
        while let Ok(entry) = walk.receiver.try_recv() {
            match entry {
                Ok(entry) => match entry {
                    Entry::Root(_, id) => {
                        if walk.follow_symlinks {
                            walk.visited.as_mut().unwrap().insert(id);
                        }
                    }
                    Entry::File(entry) => {
                        walk.ready_entries.push(Ok(entry));
                    }
                    Entry::Dir(entry, unique_id, depth) => {
                        if walk
                            .max_depth
                            .map(|max_depth| depth < max_depth)
                            .unwrap_or(true)
                        {
                            walk.queue.push((entry.path(), depth));
                        }

                        if walk.follow_symlinks {
                            walk.visited
                                .as_mut()
                                .expect("BUG: This should always be Some")
                                .insert(unique_id);
                        }
                        walk.ready_entries.push(Ok(entry));
                    }
                    Entry::Symlink(entry, link, depth) => {
                        // Guaranteed to be Some this this is a symlink entry
                        if walk
                            .visited
                            .as_ref()
                            .expect("BUG: This should always be Some")
                            .contains(&link)
                        {
                            walk.ready_entries
                                .push(Err(Error::SymlinkCycle(entry.path())));
                        } else {
                            walk.queue.push((entry.path(), depth));
                            walk.ready_entries.push(Ok(entry));
                        }
                    }
                },
                Err(e) => {
                    walk.ready_entries.push(Err(e));
                }
            }
        }

        if let Some(entry) = walk.ready_entries.pop() {
            Poll::Ready(Some(entry))
        } else if walk.queue.is_empty() && walk.counter.load(Ordering::Relaxed) == 0 {
            // We are done when ready entries is empty, there is nothing in the queue and no ongoing async tasks
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}

//pub struct WalkBuilder {}

pub struct WalkBuilder {
    root: PathBuf,
    follow_symlinks: bool,
    concurrency_limit: Option<usize>,
    max_depth: Option<u64>,
    filter: Option<Filter>,
}

impl WalkBuilder {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            follow_symlinks: false,
            concurrency_limit: None,
            max_depth: None,
            filter: None,
        }
    }

    pub fn follow_symlinks(mut self, follow_symlinks: bool) -> Self {
        self.follow_symlinks = follow_symlinks;
        self
    }

    pub fn concurrency_limit(mut self, concurrency_limit: usize) -> Self {
        self.concurrency_limit = Some(concurrency_limit);
        self
    }

    pub fn max_depth<'a>(mut self, max_depth: u64) -> Self {
        self.max_depth = Some(max_depth);
        self
    }

    // TODO: Support just passing in a closure
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    pub fn build(self) -> Walk {
        Walk::new(
            self.root,
            self.follow_symlinks,
            self.concurrency_limit,
            self.max_depth,
            self.filter,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use tempfile::{tempdir, tempdir_in, NamedTempFile};
    use tokio::fs::os::unix::symlink;

    #[tokio::test(core_threads = 4)]
    async fn test_single_level() {
        let root = tempdir().unwrap();
        let file = NamedTempFile::new_in(root.path()).unwrap();
        let file2 = NamedTempFile::new_in(root.path()).unwrap();

        let dir = tempdir_in(root.path()).unwrap();

        let walk = WalkBuilder::new(root.path()).build();
        let entries = walk
            .map(|entry| entry.ok().map(|entry| entry.path()))
            .collect::<Vec<Option<PathBuf>>>()
            .await;
        drop(root);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries.contains(&Some(file.path().to_path_buf())), true);
        assert_eq!(entries.contains(&Some(file2.path().to_path_buf())), true);
        assert_eq!(entries.contains(&Some(dir.path().to_path_buf())), true);
    }

    #[tokio::test(core_threads = 4)]
    async fn test_multi_level() {
        let root = tempdir().unwrap();
        let file = NamedTempFile::new_in(root.path()).unwrap();
        let dir = tempdir_in(root.path()).unwrap();
        let file2 = NamedTempFile::new_in(dir.path()).unwrap();

        let walk = WalkBuilder::new(root.path()).build();
        let entries = walk
            .map(|entry| entry.ok().map(|entry| entry.path()))
            .collect::<Vec<Option<PathBuf>>>()
            .await;
        drop(root);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries.contains(&Some(file.path().to_path_buf())), true);
        assert_eq!(entries.contains(&Some(file2.path().to_path_buf())), true);
        assert_eq!(entries.contains(&Some(dir.path().to_path_buf())), true);
    }

    #[tokio::test(core_threads = 4)]
    async fn test_max_depth() {
        let root = tempdir().unwrap();
        let file = NamedTempFile::new_in(root.path()).unwrap();
        let dir1 = tempdir_in(root.path()).unwrap();
        let dir2 = tempdir_in(dir1.path()).unwrap();
        let file2 = NamedTempFile::new_in(dir2.path()).unwrap();

        let walk = WalkBuilder::new(root.path().to_path_buf())
            .max_depth(2)
            .build();
        let entries = walk
            .map(|entry| entry.ok().map(|entry| entry.path()))
            .collect::<Vec<Option<PathBuf>>>()
            .await;
        drop(file);
        drop(dir1);
        drop(dir2);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries.contains(&Some(file2.path().to_path_buf())), false);
    }

    #[tokio::test(core_threads = 4)]
    async fn test_follow_symlinks() {
        let root = tempdir().unwrap();
        let linked_dir = tempdir().unwrap();
        let link = root.path().join("link");
        symlink(linked_dir.path(), &link).await.unwrap();
        let file = NamedTempFile::new_in(&link).unwrap();
        let walk = WalkBuilder::new(root.path().to_path_buf())
            .follow_symlinks(true)
            .build();
        let entries = walk
            .map(|entry| {
                entry.as_ref().unwrap();
                entry.ok().map(|entry| entry.path())
            })
            .collect::<Vec<Option<PathBuf>>>()
            .await;
        drop(root);
        drop(linked_dir);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries.contains(&Some(file.path().to_path_buf())), true);
    }

    #[tokio::test(core_threads = 4)]
    async fn test_does_not_follow_symlinks() {
        let root = tempdir().unwrap();
        let linked_dir = tempdir().unwrap();
        let file = NamedTempFile::new_in(linked_dir.path()).unwrap();
        symlink(&linked_dir, root.path().join("link"))
            .await
            .unwrap();
        let walk = WalkBuilder::new(root.path()).max_depth(2).build();
        let entries = walk
            .map(|entry| {
                entry.as_ref().unwrap();
                entry.ok().map(|entry| entry.path())
            })
            .collect::<Vec<Option<PathBuf>>>()
            .await;
        drop(root);
        drop(linked_dir);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries.contains(&Some(file.path().to_path_buf())), false);
    }

    #[tokio::test(core_threads = 4)]
    async fn test_symlink_cycle() {
        let root = tempdir().unwrap();
        let link = root.path().join("link");
        symlink(root.path(), &link).await.unwrap();
        let file = NamedTempFile::new_in(&link).unwrap();
        let walk = WalkBuilder::new(root.path()).follow_symlinks(true).build();
        let entries = walk.collect::<Vec<Result<DirEntry>>>().await;
        // Search for the error in the entries
        let find = entries.iter().find(|res| match res {
            Err(e) => match e {
                Error::SymlinkCycle(p) => p == &link,
                _ => false,
            },
            _ => false,
        });
        drop(file);
        drop(root);
        assert_eq!(entries.len(), 2);
        assert_eq!(find.is_some(), true);
    }

    #[tokio::test(core_threads = 4)]
    async fn test_filter() {
        let root = tempdir().unwrap();
        let file = NamedTempFile::new_in(root.path()).unwrap();
        let dir = tempdir_in(root.path()).unwrap();
        let file2 = NamedTempFile::new_in(dir.path()).unwrap();
        let filter_dir = dir.path().to_path_buf();
        let filter: Filter = Box::new(move |entry| {
            let filter_dir = filter_dir.clone();
            async move { FilterResult::Ok(entry.path() != filter_dir) }.boxed()
        });
        let walk = WalkBuilder::new(root.path()).filter(filter).build();
        let entries = walk
            .map(|entry| {
                entry.as_ref().unwrap();
                entry.ok().map(|entry| entry.path())
            })
            .collect::<Vec<Option<PathBuf>>>()
            .await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries.contains(&Some(file2.path().to_path_buf())), false);
        assert_eq!(entries.contains(&Some(dir.path().to_path_buf())), false);
        assert_eq!(entries.contains(&Some(file.path().to_path_buf())), true);
        drop(root);
    }

    #[tokio::test(core_threads = 4)]
    async fn test_filter_error() {
        let root = tempdir().unwrap();
        let file = NamedTempFile::new_in(root.path()).unwrap();

        let filter: Filter =
            Box::new(move |_entry| async move { FilterResult::Err("Error!!".into()) }.boxed());
        let walk = WalkBuilder::new(root.path()).filter(filter).build();
        let entries = walk.collect::<Vec<Result<DirEntry>>>().await;
        // Search for the error in the entries
        let find = entries.iter().find(|res| match res {
            Err(e) => match e {
                Error::Filter(_, _) => true,
                _ => false,
            },
            _ => false,
        });
        assert_eq!(entries.len(), 1);
        assert_eq!(find.is_some(), true);
        drop(root);
        drop(file);
    }
}
