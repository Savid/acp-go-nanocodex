use nanocodex::{
    agent::session::{SessionSnapshot, SessionSnapshotHead},
    oai::responses::ResponseItem,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

const MAX_CHECKPOINT_BYTES: u64 = 1024 * 1024;

#[derive(Deserialize, Serialize)]
struct Checkpoint {
    preceding_bytes: u64,
    history_items: usize,
    head: SessionSnapshotHead,
    prefix: Vec<ResponseItem>,
}

fn sidecar(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".acp-checkpoint.json");
    name.into()
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid native checkpoint")
}

pub fn restore(path: &Path, snapshot: SessionSnapshot) -> io::Result<SessionSnapshot> {
    let mut file = OpenOptions::new().read(true).append(true).open(path)?;
    let mut end = file.metadata()?.len();
    if end > 0 {
        file.seek(SeekFrom::End(-1))?;
        let mut last = [0];
        file.read_exact(&mut last)?;
        if last[0] != b'\n' {
            file.write_all(b"\n")?;
            file.sync_all()?;
            end += 1;
        }
    }
    match restore_checkpoint(path, end, &snapshot) {
        Ok(Some(restored)) => Ok(restored),
        Ok(None) => Ok(snapshot),
        Err(_) => {
            eprintln!("native checkpoint ignored; restoring native history");
            Ok(snapshot)
        }
    }
}

fn restore_checkpoint(
    path: &Path,
    end: u64,
    snapshot: &SessionSnapshot,
) -> io::Result<Option<SessionSnapshot>> {
    let metadata = match std::fs::symlink_metadata(sidecar(path)) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !metadata.is_file() || metadata.len() > MAX_CHECKPOINT_BYTES {
        return Err(invalid());
    }
    let file = match File::open(sidecar(path)) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > MAX_CHECKPOINT_BYTES {
        return Err(invalid());
    }
    let checkpoint: Checkpoint =
        serde_json::from_reader(file.take(MAX_CHECKPOINT_BYTES + 1)).map_err(|_| invalid())?;
    let (original, history, _) = snapshot.clone().into_context_parts();
    if checkpoint.preceding_bytes != end || checkpoint.history_items != history.len() {
        return Err(invalid());
    }
    let original = serde_json::to_value(original).map_err(|_| invalid())?;
    let restored = serde_json::to_value(&checkpoint.head).map_err(|_| invalid())?;
    for field in [
        "version",
        "model",
        "lineage_id",
        "prompt_cache_key",
        "workspace",
    ] {
        if original.get(field) != restored.get(field) {
            return Err(invalid());
        }
    }
    let prefix = serde_json::to_value(&checkpoint.prefix).map_err(|_| invalid())?;
    if checkpoint.prefix.len() != 2
        || prefix[0]["type"] != "additional_tools"
        || prefix[0]["role"] != "developer"
        || prefix[1]["type"] != "message"
        || prefix[1]["role"] != "developer"
    {
        return Err(invalid());
    }
    Ok(Some(
        checkpoint
            .head
            .with_context(history, Some(checkpoint.prefix)),
    ))
}

// The native writer must be shut down before capturing its final byte boundary.
pub fn save(path: &Path, snapshot: SessionSnapshot) -> io::Result<()> {
    let (head, history, prefix) = snapshot.into_context_parts();
    let Some(prefix) = prefix else {
        return Ok(());
    };
    let checkpoint = Checkpoint {
        preceding_bytes: path.metadata()?.len(),
        history_items: history.len(),
        head,
        prefix,
    };
    let bytes = serde_json::to_vec(&checkpoint).map_err(|_| invalid())?;
    if bytes.len() as u64 > MAX_CHECKPOINT_BYTES {
        return Err(invalid());
    }
    let destination = sidecar(path);
    let stage = destination.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&stage)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(&stage, &destination)?;
        File::open(destination.parent().ok_or_else(invalid)?)?.sync_all()
    })();
    let _ = std::fs::remove_file(stage);
    result
}
