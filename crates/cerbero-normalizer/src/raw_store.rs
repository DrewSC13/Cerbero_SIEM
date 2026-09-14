use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Component, Path, PathBuf};

use cerbero_common::contracts::v1::RawEventPersisted;

use crate::NormalizerError;

#[derive(Clone, Debug)]
pub struct FilesystemRawReader {
    root: PathBuf,
}

impl FilesystemRawReader {
    pub fn new(root: impl AsRef<Path>) -> Result<Self, NormalizerError> {
        let root = std::fs::canonicalize(root.as_ref()).map_err(|error| {
            storage_error(
                "CER-NORM-RAW-ROOT",
                format!("canonicalize Raw Store root: {error}"),
                false,
            )
        })?;
        if !root.is_dir() {
            return Err(storage_error(
                "CER-NORM-RAW-ROOT",
                "Raw Store root is not a directory".to_string(),
                false,
            ));
        }
        Ok(Self { root })
    }

    pub fn read(&self, persisted: &RawEventPersisted) -> Result<Vec<u8>, NormalizerError> {
        let relative = persisted
            .storage_uri
            .strip_prefix("raw:///")
            .ok_or_else(|| {
                storage_error(
                    "CER-NORM-RAW-URI",
                    "storage_uri must use raw:/// for the DEVELOPMENT filesystem Raw Store"
                        .to_string(),
                    false,
                )
            })?;
        let relative_path = Path::new(relative);
        if relative_path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(storage_error(
                "CER-NORM-RAW-URI",
                "storage_uri contains a non-normal path component".to_string(),
                false,
            ));
        }
        let candidate = self.root.join(relative_path);
        let canonical = std::fs::canonicalize(&candidate).map_err(|error| {
            storage_error(
                "CER-NORM-RAW-READ",
                format!("resolve durable Raw Store object: {error}"),
                true,
            )
        })?;
        if !canonical.starts_with(&self.root) {
            return Err(storage_error(
                "CER-NORM-RAW-URI",
                "storage_uri resolves outside the configured Raw Store root".to_string(),
                false,
            ));
        }
        let mut file = File::open(&canonical).map_err(|error| {
            storage_error(
                "CER-NORM-RAW-READ",
                format!("open durable Raw Store object: {error}"),
                true,
            )
        })?;
        file.seek(SeekFrom::Start(persisted.offset))
            .map_err(|error| {
                storage_error(
                    "CER-NORM-RAW-READ",
                    format!("seek durable Raw Store object: {error}"),
                    true,
                )
            })?;
        let length = usize::try_from(persisted.length).map_err(|_| {
            storage_error(
                "CER-NORM-RAW-LENGTH",
                "raw locator length exceeds this platform address space".to_string(),
                false,
            )
        })?;
        let mut bytes = vec![0_u8; length];
        file.read_exact(&mut bytes).map_err(|error| {
            storage_error(
                "CER-NORM-RAW-READ",
                format!("read exact durable Raw Store bytes: {error}"),
                true,
            )
        })?;
        Ok(bytes)
    }
}

fn storage_error(code: &'static str, message: String, retryable: bool) -> NormalizerError {
    NormalizerError {
        code,
        message,
        retryable,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn reader_resolves_governed_raw_uri_and_range() {
        let root = TempDir::new().unwrap();
        let path = root.path().join("tenant/2026/09/13/12/segment/raw.bin");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, b"abcdef").unwrap();
        let reader = FilesystemRawReader::new(root.path()).unwrap();
        let persisted = RawEventPersisted {
            storage_uri: "raw:///tenant/2026/09/13/12/segment/raw.bin".to_string(),
            offset: 1,
            length: 3,
            ..Default::default()
        };
        assert_eq!(reader.read(&persisted).unwrap(), b"bcd");
    }

    #[test]
    fn reader_rejects_path_escape() {
        let root = TempDir::new().unwrap();
        let reader = FilesystemRawReader::new(root.path()).unwrap();
        let persisted = RawEventPersisted {
            storage_uri: "raw:///../outside".to_string(),
            length: 1,
            ..Default::default()
        };
        assert_eq!(
            reader.read(&persisted).unwrap_err().code,
            "CER-NORM-RAW-URI"
        );
    }
}
