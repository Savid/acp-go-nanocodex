use nanocodex::{
    agent::session::{SessionSnapshot, SessionSnapshotHead},
    oai::responses::ResponseItem,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

const RECORD_TYPE: &str = "acp_checkpoint";

#[derive(Deserialize, Serialize)]
struct Checkpoint {
    preceding_bytes: u64,
    history_items: usize,
    head: SessionSnapshotHead,
    prefix: Vec<ResponseItem>,
}

struct Tail {
    record: Value,
    start: u64,
    end: u64,
    terminated: bool,
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid native checkpoint")
}

fn tail(file: &mut File) -> io::Result<Tail> {
    let end = file.metadata()?.len();
    let limit = end.min(crate::MAX_FRAME_BYTES as u64 + 2);
    let mut count = limit.min(8192);
    loop {
        file.seek(SeekFrom::End(-(count as i64)))?;
        let mut bytes = vec![0; count as usize];
        file.read_exact(&mut bytes)?;
        let terminated = bytes.last() == Some(&b'\n');
        if terminated {
            bytes.pop();
        }
        let start = match bytes.iter().rposition(|byte| *byte == b'\n') {
            Some(index) => index + 1,
            None if count == end => 0,
            None if count < limit => {
                count = (count * 2).min(limit);
                continue;
            }
            None => return Err(invalid()),
        };
        if bytes.len() - start > crate::MAX_FRAME_BYTES {
            return Err(invalid());
        }
        return Ok(Tail {
            record: serde_json::from_slice(&bytes[start..]).map_err(|_| invalid())?,
            start: end - count + start as u64,
            end,
            terminated,
        });
    }
}

pub fn restore(path: &Path, snapshot: SessionSnapshot) -> io::Result<SessionSnapshot> {
    let mut file = OpenOptions::new().read(true).append(true).open(path)?;
    let tail = tail(&mut file)?;
    let snapshot = if tail.record["type"] == RECORD_TYPE {
        let checkpoint: Checkpoint =
            serde_json::from_value(tail.record["payload"].clone()).map_err(|_| invalid())?;
        let (original, history, _) = snapshot.into_context_parts();
        if checkpoint.preceding_bytes != tail.start || checkpoint.history_items != history.len() {
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
        checkpoint
            .head
            .with_context(history, Some(checkpoint.prefix))
    } else {
        snapshot
    };
    if !tail.terminated {
        append_bytes(&mut file, tail.end, b"\n")?;
    }
    Ok(snapshot)
}

// The native writer must be shut down before this appends to its rollout.
pub fn append(path: &Path, snapshot: SessionSnapshot) -> io::Result<()> {
    let (head, history, prefix) = snapshot.into_context_parts();
    let Some(prefix) = prefix else {
        return Ok(());
    };
    let mut file = OpenOptions::new().read(true).append(true).open(path)?;
    let tail = tail(&mut file)?;
    let checkpoint = Checkpoint {
        preceding_bytes: tail.end + u64::from(!tail.terminated),
        history_items: history.len(),
        head,
        prefix,
    };
    let payload = serde_json::to_value(checkpoint).map_err(|_| invalid())?;
    let timestamp = tail.record["timestamp"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(invalid)?;
    let mut row = serde_json::to_vec(&json!({
        "timestamp":timestamp,"type":RECORD_TYPE,"payload":payload,
    }))
    .map_err(|_| invalid())?;
    if row.len() > crate::MAX_FRAME_BYTES {
        return Err(invalid());
    }
    if !tail.terminated {
        row.insert(0, b'\n');
    }
    row.push(b'\n');
    append_bytes(&mut file, tail.end, &row)
}

fn append_bytes(file: &mut File, original_len: u64, bytes: &[u8]) -> io::Result<()> {
    if file.metadata()?.len() != original_len {
        return Err(invalid());
    }
    if let Err(error) = file.write_all(bytes).and_then(|()| file.sync_all()) {
        file.set_len(original_len)?;
        file.sync_all()?;
        return Err(error);
    }
    Ok(())
}
