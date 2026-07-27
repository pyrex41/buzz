//! Local-filesystem media backend.
//!
//! Key layout mirrors the S3 backend byte-for-byte
//! (`{sha}.{ext}`, `{sha}.thumb.jpg`, `_meta/{community}/{sha}.json`,
//! `_uploads/{community}/{sha}/{event_id}.json`) so `bucket_index` folds and
//! sidecar tenancy gates work unchanged. Writes are tmp-file + atomic rename
//! within the target directory, so readers never observe partial blobs.

use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncSeekExt};

use crate::error::MediaError;
use crate::storage::{BlobHeadMeta, ByteStream};

/// Local-filesystem object store rooted at one directory.
pub struct LocalFsStorage {
    root: PathBuf,
}

impl LocalFsStorage {
    /// Create the store, ensuring `root` exists.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, MediaError> {
        let root = root.into();
        std::fs::create_dir_all(&root)
            .map_err(|e| MediaError::StorageError(format!("create media root: {e}")))?;
        Ok(Self { root })
    }

    /// Resolve an object key to a path under the root, rejecting traversal.
    ///
    /// Keys are relay-internal (hex hashes, UUID-scoped sidecars), so a
    /// traversal attempt is a bug — fail loudly rather than write outside
    /// the root.
    fn key_path(&self, key: &str) -> Result<PathBuf, MediaError> {
        if key.is_empty()
            || key.starts_with('/')
            || key
                .split('/')
                .any(|seg| seg.is_empty() || seg == "." || seg == "..")
        {
            return Err(MediaError::StorageError(format!(
                "invalid object key: {key:?}"
            )));
        }
        Ok(self.root.join(key))
    }

    async fn prepare_parent(path: &Path) -> Result<(), MediaError> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| MediaError::StorageError(format!("create dir: {e}")))?;
        }
        Ok(())
    }

    /// Atomically materialize `path` from a same-directory temp file the
    /// caller has fully written.
    async fn commit_tmp(tmp: PathBuf, path: &Path) -> Result<(), MediaError> {
        tokio::fs::rename(&tmp, path)
            .await
            .map_err(|e| MediaError::StorageError(format!("rename into place: {e}")))
    }

    fn tmp_sibling(path: &Path) -> PathBuf {
        let mut name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "blob".to_string());
        name.push_str(&format!(".tmp-{}", uuid::Uuid::new_v4()));
        path.with_file_name(name)
    }

    pub async fn put(&self, key: &str, bytes: &[u8]) -> Result<(), MediaError> {
        let path = self.key_path(key)?;
        Self::prepare_parent(&path).await?;
        let tmp = Self::tmp_sibling(&path);
        tokio::fs::write(&tmp, bytes)
            .await
            .map_err(|e| MediaError::StorageError(format!("write blob: {e}")))?;
        Self::commit_tmp(tmp, &path).await
    }

    pub async fn put_file(&self, key: &str, source: &Path) -> Result<(), MediaError> {
        let path = self.key_path(key)?;
        Self::prepare_parent(&path).await?;
        let tmp = Self::tmp_sibling(&path);
        tokio::fs::copy(source, &tmp)
            .await
            .map_err(|e| MediaError::StorageError(format!("copy blob: {e}")))?;
        Self::commit_tmp(tmp, &path).await
    }

    pub async fn get(&self, key: &str) -> Result<Vec<u8>, MediaError> {
        let path = self.key_path(key)?;
        match tokio::fs::read(&path).await {
            Ok(bytes) => Ok(bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(MediaError::NotFound),
            Err(e) => Err(MediaError::StorageError(format!("read blob: {e}"))),
        }
    }

    /// Inclusive byte range, mirroring S3 `Range` GET semantics. Reads past
    /// the end are truncated to the available bytes.
    pub async fn get_range(&self, key: &str, start: u64, end: u64) -> Result<Vec<u8>, MediaError> {
        let path = self.key_path(key)?;
        let mut file = match tokio::fs::File::open(&path).await {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(MediaError::NotFound),
            Err(e) => return Err(MediaError::StorageError(format!("open blob: {e}"))),
        };
        file.seek(std::io::SeekFrom::Start(start))
            .await
            .map_err(|e| MediaError::StorageError(format!("seek blob: {e}")))?;
        let len = end.saturating_sub(start).saturating_add(1);
        let mut buf = Vec::with_capacity(len.min(8 * 1024 * 1024) as usize);
        let mut limited = file.take(len);
        limited
            .read_to_end(&mut buf)
            .await
            .map_err(|e| MediaError::StorageError(format!("read range: {e}")))?;
        Ok(buf)
    }

    pub async fn get_stream(&self, key: &str) -> Result<ByteStream, MediaError> {
        let path = self.key_path(key)?;
        let file = match tokio::fs::File::open(&path).await {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(MediaError::NotFound),
            Err(e) => return Err(MediaError::StorageError(format!("open blob: {e}"))),
        };
        let stream =
            futures_util::StreamExt::map(tokio_util::io::ReaderStream::new(file), |chunk| {
                chunk.map_err(|e| MediaError::StorageError(e.to_string()))
            });
        Ok(Box::pin(stream))
    }

    pub async fn head(&self, key: &str) -> Result<bool, MediaError> {
        Ok(self.head_with_metadata(key).await?.is_some())
    }

    pub async fn head_with_metadata(&self, key: &str) -> Result<Option<BlobHeadMeta>, MediaError> {
        let path = self.key_path(key)?;
        match tokio::fs::metadata(&path).await {
            Ok(meta) if meta.is_file() => Ok(Some(BlobHeadMeta { size: meta.len() })),
            Ok(_) => Ok(None),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(MediaError::StorageError(format!("stat blob: {e}"))),
        }
    }

    pub async fn delete(&self, key: &str) -> Result<(), MediaError> {
        let path = self.key_path(key)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(MediaError::StorageError(format!("delete blob: {e}"))),
        }
    }

    /// One page of a sorted full-tree listing. The continuation token is the
    /// last key of the previous page (keys strictly greater follow), giving
    /// the same stable pagination shape as S3's `list_page`.
    pub async fn list_page(
        &self,
        continuation_token: Option<String>,
        max_keys: usize,
    ) -> Result<crate::bucket_index::Page, MediaError> {
        // Collect keys with a blocking walk — bounded by what's on disk for
        // a single-node deployment, and the sweep caps cumulative objects.
        let root = self.root.clone();
        let mut keys = tokio::task::spawn_blocking(move || walk_keys(&root))
            .await
            .map_err(|e| MediaError::StorageError(format!("walk join: {e}")))??;
        keys.sort();

        let start_index = match &continuation_token {
            Some(token) => keys.partition_point(|(key, _)| key <= token),
            None => 0,
        };
        let page: Vec<(String, u64)> = keys
            .iter()
            .skip(start_index)
            .take(max_keys)
            .cloned()
            .collect();
        let is_truncated = start_index + page.len() < keys.len();
        let next_continuation_token = if is_truncated {
            page.last().map(|(key, _)| key.clone())
        } else {
            None
        };
        Ok(crate::bucket_index::Page {
            objects: page,
            next_continuation_token,
            is_truncated,
        })
    }
}

/// Recursively collect `(relative_key, size)` for every file under `root`,
/// skipping in-flight `.tmp-*` writes.
fn walk_keys(root: &Path) -> Result<Vec<(String, u64)>, MediaError> {
    let mut keys = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let entries = std::fs::read_dir(&dir)
            .map_err(|e| MediaError::StorageError(format!("read dir: {e}")))?;
        for entry in entries {
            let entry = entry.map_err(|e| MediaError::StorageError(format!("read entry: {e}")))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|e| MediaError::StorageError(format!("file type: {e}")))?;
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file() {
                let name = entry.file_name();
                if name.to_string_lossy().contains(".tmp-") {
                    continue;
                }
                let size = entry
                    .metadata()
                    .map_err(|e| MediaError::StorageError(format!("stat: {e}")))?
                    .len();
                let key = path
                    .strip_prefix(root)
                    .map_err(|e| MediaError::StorageError(format!("strip prefix: {e}")))?
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy())
                    .collect::<Vec<_>>()
                    .join("/");
                keys.push((key, size));
            }
        }
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, LocalFsStorage) {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalFsStorage::new(dir.path()).expect("store");
        (dir, store)
    }

    #[tokio::test]
    async fn put_get_head_delete_round_trip() {
        let (_dir, store) = store();
        assert!(store.get("abc123.png").await.is_err());
        store.put("abc123.png", b"png-bytes").await.unwrap();
        assert_eq!(store.get("abc123.png").await.unwrap(), b"png-bytes");
        assert!(store.head("abc123.png").await.unwrap());
        assert_eq!(
            store
                .head_with_metadata("abc123.png")
                .await
                .unwrap()
                .unwrap()
                .size,
            9
        );
        store.delete("abc123.png").await.unwrap();
        assert!(!store.head("abc123.png").await.unwrap());
        assert!(matches!(
            store.get("abc123.png").await,
            Err(MediaError::NotFound)
        ));
    }

    #[tokio::test]
    async fn nested_sidecar_keys_create_directories() {
        let (_dir, store) = store();
        store
            .put("_meta/00000000-0000-0000-0000-000000000001/abc.json", b"{}")
            .await
            .unwrap();
        assert_eq!(
            store
                .get("_meta/00000000-0000-0000-0000-000000000001/abc.json")
                .await
                .unwrap(),
            b"{}"
        );
    }

    #[tokio::test]
    async fn range_reads_are_inclusive_and_clamped() {
        let (_dir, store) = store();
        store.put("blob.bin", b"0123456789").await.unwrap();
        assert_eq!(store.get_range("blob.bin", 2, 5).await.unwrap(), b"2345");
        assert_eq!(store.get_range("blob.bin", 8, 100).await.unwrap(), b"89");
        assert_eq!(store.get_range("blob.bin", 0, 0).await.unwrap(), b"0");
    }

    #[tokio::test]
    async fn traversal_keys_are_rejected() {
        let (_dir, store) = store();
        for bad in ["../escape", "/abs", "a/../b", "", "a//b"] {
            assert!(store.put(bad, b"x").await.is_err(), "must reject {bad:?}");
        }
    }

    #[tokio::test]
    async fn list_page_paginates_in_key_order() {
        let (_dir, store) = store();
        for key in ["b.bin", "a.bin", "_meta/c/d.json"] {
            store.put(key, b"x").await.unwrap();
        }
        let page1 = store.list_page(None, 2).await.unwrap();
        assert_eq!(
            page1
                .objects
                .iter()
                .map(|(k, _)| k.as_str())
                .collect::<Vec<_>>(),
            vec!["_meta/c/d.json", "a.bin"]
        );
        assert!(page1.is_truncated);
        let page2 = store
            .list_page(page1.next_continuation_token, 2)
            .await
            .unwrap();
        assert_eq!(
            page2
                .objects
                .iter()
                .map(|(k, _)| k.as_str())
                .collect::<Vec<_>>(),
            vec!["b.bin"]
        );
        assert!(!page2.is_truncated);
        assert!(page2.next_continuation_token.is_none());
    }

    #[tokio::test]
    async fn stream_returns_full_contents() {
        use futures_util::StreamExt;
        let (_dir, store) = store();
        store.put("video.mp4", b"streamed-bytes").await.unwrap();
        let mut stream = store.get_stream("video.mp4").await.unwrap();
        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await {
            collected.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(collected, b"streamed-bytes");
    }
}
