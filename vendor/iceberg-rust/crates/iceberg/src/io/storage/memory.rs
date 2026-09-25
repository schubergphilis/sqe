// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Pure Rust in-memory storage implementation for testing.
//!
//! This module provides a `MemoryStorage` implementation that stores data
//! in a thread-safe `HashMap`, without any external dependencies.
//! It is primarily intended for unit testing and scenarios where persistent
//! storage is not needed.

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use serde::{Deserialize, Serialize};

use crate::error::invalid_data;
use crate::io::{
    FileMetadata, FileRead, FileWrite, InputFile, ListEntry, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use crate::{Error, ErrorKind, Result};

/// In-memory storage implementation.
///
/// This storage implementation stores all data in a thread-safe `HashMap`,
/// making it suitable for unit tests and scenarios where persistent storage
/// is not needed.
///
/// # Path Normalization
///
/// The storage normalizes paths to handle various formats:
/// - `memory://path/to/file` -> `path/to/file`
/// - `memory:/path/to/file` -> `path/to/file`
/// - `/path/to/file` -> `path/to/file`
/// - `path/to/file` -> `path/to/file`
///
/// # Serialization
///
/// When serialized, `MemoryStorage` serializes to an empty state. When
/// deserialized, it creates a new empty instance. This is intentional
/// because in-memory data cannot be meaningfully serialized across
/// process boundaries.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryStorage {
    #[serde(skip, default = "default_memory_data")]
    data: Arc<RwLock<HashMap<String, Bytes>>>,
    #[serde(skip, default = "default_memory_timestamps")]
    last_modified_ms: Arc<RwLock<HashMap<String, i64>>>,
}

fn default_memory_data() -> Arc<RwLock<HashMap<String, Bytes>>> {
    Arc::new(RwLock::new(HashMap::new()))
}

fn default_memory_timestamps() -> Arc<RwLock<HashMap<String, i64>>> {
    Arc::new(RwLock::new(HashMap::new()))
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

impl MemoryStorage {
    /// Create a new empty `MemoryStorage` instance.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            last_modified_ms: default_memory_timestamps(),
        }
    }

    /// Normalize a path by removing scheme prefixes and leading slashes.
    ///
    /// This handles the following formats:
    /// - `memory://path` -> `path`
    /// - `memory:/path` -> `path`
    /// - `/path` -> `path`
    /// - `path` -> `path`
    pub(crate) fn normalize_path(path: &str) -> String {
        // Handle memory:// prefix (with double slash)
        let path = path.strip_prefix("memory://").unwrap_or(path);
        // Handle memory:/ prefix (with single slash)
        let path = path.strip_prefix("memory:/").unwrap_or(path);
        // Remove any leading slashes
        path.trim_start_matches('/').to_string()
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for MemoryStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let normalized = Self::normalize_path(path);
        let data = self.data.read().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire read lock: {e}"),
            )
        })?;
        Ok(data.contains_key(&normalized))
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let normalized = Self::normalize_path(path);
        let data = self.data.read().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire read lock: {e}"),
            )
        })?;
        match data.get(&normalized) {
            Some(bytes) => Ok(FileMetadata {
                size: bytes.len() as u64,
            }),
            None => Err(invalid_data!("File not found: {path}")),
        }
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let normalized = Self::normalize_path(path);
        let data = self.data.read().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire read lock: {e}"),
            )
        })?;
        match data.get(&normalized) {
            Some(bytes) => Ok(bytes.clone()),
            None => Err(invalid_data!("File not found: {path}")),
        }
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let normalized = Self::normalize_path(path);
        let data = self.data.read().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire read lock: {e}"),
            )
        })?;
        match data.get(&normalized) {
            Some(bytes) => Ok(Box::new(MemoryFileRead::new(bytes.clone()))),
            None => Err(invalid_data!("File not found: {path}")),
        }
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let normalized = Self::normalize_path(path);
        let mut data = self.data.write().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire write lock: {e}"),
            )
        })?;
        data.insert(normalized.clone(), bs);
        drop(data);
        self.last_modified_ms
            .write()
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to acquire timestamp write lock: {e}"),
                )
            })?
            .insert(normalized, now_ms());
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let normalized = Self::normalize_path(path);
        Ok(Box::new(MemoryFileWrite::new_with_timestamps(
            self.data.clone(),
            self.last_modified_ms.clone(),
            normalized,
        )))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let normalized = Self::normalize_path(path);
        let mut data = self.data.write().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire write lock: {e}"),
            )
        })?;
        data.remove(&normalized);
        drop(data);
        self.last_modified_ms
            .write()
            .map_err(|e| {
                Error::new(
                    ErrorKind::Unexpected,
                    format!("Failed to acquire timestamp write lock: {e}"),
                )
            })?
            .remove(&normalized);
        Ok(())
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let normalized = Self::normalize_path(path);
        let prefix = if normalized.ends_with('/') {
            normalized
        } else {
            format!("{normalized}/")
        };

        let mut data = self.data.write().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire write lock: {e}"),
            )
        })?;

        // Collect keys to remove (can't modify while iterating)
        let keys_to_remove: Vec<String> = data
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();

        for key in keys_to_remove {
            data.remove(&key);
        }
        drop(data);
        let mut timestamps = self.last_modified_ms.write().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire timestamp write lock: {e}"),
            )
        })?;
        timestamps.retain(|key, _| !key.starts_with(&prefix));

        Ok(())
    }

    async fn delete_stream(&self, mut paths: BoxStream<'static, String>) -> Result<()> {
        while let Some(path) = paths.next().await {
            self.delete(&path).await?;
        }
        Ok(())
    }

    async fn list(
        &self,
        path: &str,
        recursive: bool,
    ) -> Result<BoxStream<'static, Result<ListEntry>>> {
        let normalized = Self::normalize_path(path);
        let prefix = if normalized.is_empty() {
            String::new()
        } else {
            format!("{}/", normalized.trim_end_matches('/'))
        };
        let base = path.trim_end_matches('/');
        let data = self.data.read().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire read lock: {e}"),
            )
        })?;
        let timestamps = self.last_modified_ms.read().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire timestamp read lock: {e}"),
            )
        })?;

        let mut entries = Vec::new();
        let mut directories = HashSet::new();
        for (stored_path, bytes) in data.iter() {
            let Some(relative) = stored_path.strip_prefix(&prefix) else {
                continue;
            };
            if relative.is_empty() {
                continue;
            }

            if !recursive && let Some((directory, _)) = relative.split_once('/') {
                if directories.insert(directory.to_string()) {
                    entries.push(ListEntry {
                        path: format!("{base}/{directory}"),
                        size: 0,
                        last_modified_ms: None,
                        is_dir: true,
                    });
                }
                continue;
            }

            entries.push(ListEntry {
                path: format!("{base}/{relative}"),
                size: bytes.len() as u64,
                last_modified_ms: timestamps.get(stored_path).copied(),
                is_dir: false,
            });
        }
        entries.sort_unstable_by(|left, right| left.path.cmp(&right.path));
        Ok(stream::iter(entries.into_iter().map(Ok)).boxed())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

/// Factory for creating `MemoryStorage` instances.
///
/// This factory implements `StorageFactory` and creates `MemoryStorage`
/// instances. Since the factory is explicitly chosen, no scheme validation
/// is performed - the storage will validate paths during operations.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MemoryStorageFactory;

#[typetag::serde]
impl StorageFactory for MemoryStorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(MemoryStorage::new()))
    }
}

/// File reader for in-memory storage.
#[derive(Debug)]
pub struct MemoryFileRead {
    data: Bytes,
}

impl MemoryFileRead {
    /// Create a new `MemoryFileRead` with the given data.
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }
}

#[async_trait]
impl FileRead for MemoryFileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        let start = range.start as usize;
        let end = range.end as usize;

        if start > self.data.len() || end > self.data.len() {
            return Err(invalid_data!(
                "Range {}..{} is out of bounds for data of length {}",
                start,
                end,
                self.data.len()
            ));
        }

        Ok(self.data.slice(start..end))
    }
}

/// File writer for in-memory storage.
///
/// This struct implements `FileWrite` for writing to in-memory storage.
/// Data is buffered until `close()` is called, at which point it is
/// flushed to the storage.
#[derive(Debug)]
pub struct MemoryFileWrite {
    data: Arc<RwLock<HashMap<String, Bytes>>>,
    last_modified_ms: Option<Arc<RwLock<HashMap<String, i64>>>>,
    path: String,
    buffer: Vec<u8>,
    closed: bool,
}

impl MemoryFileWrite {
    /// Create a new `MemoryFileWrite` for the given path.
    pub fn new(data: Arc<RwLock<HashMap<String, Bytes>>>, path: String) -> Self {
        Self {
            data,
            last_modified_ms: None,
            path,
            buffer: Vec::new(),
            closed: false,
        }
    }

    fn new_with_timestamps(
        data: Arc<RwLock<HashMap<String, Bytes>>>,
        last_modified_ms: Arc<RwLock<HashMap<String, i64>>>,
        path: String,
    ) -> Self {
        let mut writer = Self::new(data, path);
        writer.last_modified_ms = Some(last_modified_ms);
        writer
    }
}

#[async_trait]
impl FileWrite for MemoryFileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        if self.closed {
            return Err(invalid_data!("Cannot write to closed file"));
        }
        self.buffer.extend_from_slice(&bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<FileMetadata> {
        if self.closed {
            return Err(invalid_data!("File already closed"));
        }

        let mut data = self.data.write().map_err(|e| {
            Error::new(
                ErrorKind::Unexpected,
                format!("Failed to acquire write lock: {e}"),
            )
        })?;

        let size = self.buffer.len() as u64;
        data.insert(
            self.path.clone(),
            Bytes::from(std::mem::take(&mut self.buffer)),
        );
        drop(data);
        if let Some(timestamps) = &self.last_modified_ms {
            timestamps
                .write()
                .map_err(|e| {
                    Error::new(
                        ErrorKind::Unexpected,
                        format!("Failed to acquire timestamp write lock: {e}"),
                    )
                })?
                .insert(self.path.clone(), now_ms());
        }
        self.closed = true;
        Ok(FileMetadata { size })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        // Test memory:// prefix
        assert_eq!(
            MemoryStorage::normalize_path("memory://path/to/file"),
            "path/to/file"
        );

        // Test memory:/ prefix
        assert_eq!(
            MemoryStorage::normalize_path("memory:/path/to/file"),
            "path/to/file"
        );

        // Test leading slash
        assert_eq!(
            MemoryStorage::normalize_path("/path/to/file"),
            "path/to/file"
        );

        // Test bare path
        assert_eq!(
            MemoryStorage::normalize_path("path/to/file"),
            "path/to/file"
        );

        // Test multiple leading slashes
        assert_eq!(
            MemoryStorage::normalize_path("///path/to/file"),
            "path/to/file"
        );

        // Test memory:// with leading slash in path
        assert_eq!(
            MemoryStorage::normalize_path("memory:///path/to/file"),
            "path/to/file"
        );
    }

    #[tokio::test]
    async fn test_memory_storage_write_read() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";
        let content = Bytes::from("Hello, World!");

        // Write
        storage.write(path, content.clone()).await.unwrap();

        // Read
        let read_content = storage.read(path).await.unwrap();
        assert_eq!(read_content, content);
    }

    #[tokio::test]
    async fn test_memory_storage_exists() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";

        // File doesn't exist initially
        assert!(!storage.exists(path).await.unwrap());

        // Write file
        storage.write(path, Bytes::from("test")).await.unwrap();

        // File exists now
        assert!(storage.exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_storage_metadata() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";
        let content = Bytes::from("Hello, World!");

        storage.write(path, content.clone()).await.unwrap();

        let metadata = storage.metadata(path).await.unwrap();
        assert_eq!(metadata.size, content.len() as u64);
    }

    #[tokio::test]
    async fn test_memory_storage_delete() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";

        storage.write(path, Bytes::from("test")).await.unwrap();
        assert!(storage.exists(path).await.unwrap());

        storage.delete(path).await.unwrap();
        assert!(!storage.exists(path).await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_storage_delete_prefix() {
        let storage = MemoryStorage::new();

        // Create multiple files
        storage
            .write("memory://dir/file1.txt", Bytes::from("1"))
            .await
            .unwrap();
        storage
            .write("memory://dir/file2.txt", Bytes::from("2"))
            .await
            .unwrap();
        storage
            .write("memory://other/file.txt", Bytes::from("3"))
            .await
            .unwrap();

        // Delete prefix
        storage.delete_prefix("memory://dir").await.unwrap();

        // Files in dir should be deleted
        assert!(!storage.exists("memory://dir/file1.txt").await.unwrap());
        assert!(!storage.exists("memory://dir/file2.txt").await.unwrap());

        // File in other dir should still exist
        assert!(storage.exists("memory://other/file.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_storage_reader() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";
        let content = Bytes::from("Hello, World!");

        storage.write(path, content.clone()).await.unwrap();

        let reader = storage.reader(path).await.unwrap();
        let read_content = reader.read(0..content.len() as u64).await.unwrap();
        assert_eq!(read_content, content);

        // Test partial read
        let partial = reader.read(0..5).await.unwrap();
        assert_eq!(partial, Bytes::from("Hello"));
    }

    #[tokio::test]
    async fn test_memory_storage_writer() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";

        let mut writer = storage.writer(path).await.unwrap();
        writer.write(Bytes::from("Hello, ")).await.unwrap();
        writer.write(Bytes::from("World!")).await.unwrap();
        let metadata = writer.close().await.unwrap();

        let content = storage.read(path).await.unwrap();
        assert_eq!(content, Bytes::from("Hello, World!"));
        assert_eq!(metadata.size, content.len() as u64);
    }

    #[tokio::test]
    async fn test_memory_file_write_double_close() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";

        let mut writer = storage.writer(path).await.unwrap();
        writer.write(Bytes::from("test")).await.unwrap();
        writer.close().await.unwrap();

        // Second close should fail
        let result = writer.close().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_file_write_after_close() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";

        let mut writer = storage.writer(path).await.unwrap();
        assert_eq!(writer.close().await.unwrap().size, 0);

        // Write after close should fail
        let result = writer.write(Bytes::from("test")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_memory_file_read_out_of_bounds() {
        let storage = MemoryStorage::new();
        let path = "memory://test/file.txt";
        let content = Bytes::from("Hello");

        storage.write(path, content).await.unwrap();

        let reader = storage.reader(path).await.unwrap();
        let result = reader.read(0..100).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_memory_storage_serialization() {
        let storage = MemoryStorage::new();

        // Serialize
        let serialized = serde_json::to_string(&storage).unwrap();

        // Deserialize
        let deserialized: MemoryStorage = serde_json::from_str(&serialized).unwrap();

        // Deserialized storage should be empty (new instance)
        assert!(deserialized.data.read().unwrap().is_empty());
    }

    #[test]
    fn test_memory_storage_factory() {
        let factory = MemoryStorageFactory;
        let config = StorageConfig::new();
        let storage = factory.build(&config).unwrap();

        // Verify we got a valid storage instance
        assert!(format!("{storage:?}").contains("MemoryStorage"));
    }

    #[test]
    fn test_memory_storage_factory_serialization() {
        let factory = MemoryStorageFactory;

        // Serialize
        let serialized = serde_json::to_string(&factory).unwrap();

        // Deserialize
        let deserialized: MemoryStorageFactory = serde_json::from_str(&serialized).unwrap();

        // Verify the deserialized factory works
        let config = StorageConfig::new();
        let storage = deserialized.build(&config).unwrap();
        assert!(format!("{storage:?}").contains("MemoryStorage"));
    }

    #[tokio::test]
    async fn test_path_normalization_consistency() {
        let storage = MemoryStorage::new();
        let content = Bytes::from("test content");

        // Write with one format
        storage
            .write("memory://path/to/file", content.clone())
            .await
            .unwrap();

        // Read with different formats - all should work
        assert_eq!(
            storage.read("memory://path/to/file").await.unwrap(),
            content
        );
        assert_eq!(storage.read("memory:/path/to/file").await.unwrap(), content);
        assert_eq!(storage.read("/path/to/file").await.unwrap(), content);
        assert_eq!(storage.read("path/to/file").await.unwrap(), content);
    }

    #[tokio::test]
    async fn test_memory_storage_delete_stream() {
        use futures::stream;

        let storage = MemoryStorage::new();

        // Create multiple files
        storage
            .write("memory://file1.txt", Bytes::from("1"))
            .await
            .unwrap();
        storage
            .write("memory://file2.txt", Bytes::from("2"))
            .await
            .unwrap();
        storage
            .write("memory://file3.txt", Bytes::from("3"))
            .await
            .unwrap();

        // Verify files exist
        assert!(storage.exists("memory://file1.txt").await.unwrap());
        assert!(storage.exists("memory://file2.txt").await.unwrap());
        assert!(storage.exists("memory://file3.txt").await.unwrap());

        // Delete multiple files using stream
        let paths = vec![
            "memory://file1.txt".to_string(),
            "memory://file2.txt".to_string(),
        ];
        let path_stream = stream::iter(paths).boxed();
        storage.delete_stream(path_stream).await.unwrap();

        // Verify deleted files no longer exist
        assert!(!storage.exists("memory://file1.txt").await.unwrap());
        assert!(!storage.exists("memory://file2.txt").await.unwrap());

        // Verify file3 still exists
        assert!(storage.exists("memory://file3.txt").await.unwrap());
    }

    #[tokio::test]
    async fn test_memory_storage_delete_stream_empty() {
        use futures::stream;

        let storage = MemoryStorage::new();

        // Delete with empty stream should succeed
        let path_stream = stream::iter(Vec::<String>::new()).boxed();
        storage.delete_stream(path_stream).await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_storage_list() {
        use futures::TryStreamExt;

        let storage = MemoryStorage::new();
        storage
            .write("memory://root/direct.txt", Bytes::from_static(b"a"))
            .await
            .unwrap();
        storage
            .write(
                "memory://root/nested/child.txt",
                Bytes::from_static(b"child"),
            )
            .await
            .unwrap();

        let direct: Vec<_> = storage
            .list("memory://root", false)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(direct.len(), 2);
        assert!(
            direct
                .iter()
                .any(|entry| entry.path == "memory://root/nested" && entry.is_dir)
        );

        let recursive: Vec<_> = storage
            .list("memory://root", true)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let child = recursive
            .iter()
            .find(|entry| entry.path == "memory://root/nested/child.txt")
            .unwrap();
        assert_eq!(child.size, 5);
        assert!(child.last_modified_ms.is_some());
        assert!(!child.is_dir);
    }
}
