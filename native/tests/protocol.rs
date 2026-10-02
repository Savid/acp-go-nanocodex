use serde_json::{Value, json};
use std::{io, process::Stdio, time::Duration};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    time::timeout,
};

const DEADLINE: Duration = Duration::from_secs(20);
const FRAME_LIMIT: usize = 32 * 1024 * 1024;

async fn exchange(input: &[u8]) -> (Vec<Value>, io::Result<()>) {
    let directory = TempDir::new().expect("isolated helper directory");
    let mut child = Command::new(env!("CARGO_BIN_EXE_acp-go-nanocodex-native"))
        .env_clear()
        .env("HOME", directory.path())
        .env("CODEX_HOME", directory.path())
        .current_dir(directory.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("start helper");
    let mut stdin = child.stdin.take().expect("helper stdin");
    let mut stdout = child.stdout.take().expect("helper stdout");
    let mut stderr = child.stderr.take().expect("helper stderr");
    let mut output = Vec::new();
    let mut diagnostics = Vec::new();
    let result = timeout(DEADLINE, async {
        tokio::join!(
            async {
                let result = stdin.write_all(input).await;
                drop(stdin);
                result
            },
            stdout.read_to_end(&mut output),
            stderr.read_to_end(&mut diagnostics),
            child.wait(),
        )
    })
    .await;
    let (sent, read_output, read_diagnostics, status) = match result {
        Ok(result) => result,
        Err(_) => {
            timeout(DEADLINE, child.kill())
                .await
                .expect("helper kill deadline")
                .expect("kill and reap timed-out helper");
            panic!("helper protocol deadline exceeded");
        }
    };
    read_output.expect("read helper stdout");
    read_diagnostics.expect("read helper stderr");
    assert!(
        status.expect("reap helper").success(),
        "helper failed: {}",
        String::from_utf8_lossy(&diagnostics)
    );
    assert!(output.ends_with(b"\n"), "reply must end with a newline");
    let frames = output[..output.len() - 1]
        .split(|byte| *byte == b'\n')
        .map(|line| serde_json::from_slice(line).expect("helper stdout must contain JSONL only"))
        .collect();
    (frames, sent)
}

fn rejected(id: Value) -> Value {
    json!({"id":id,"error":{"code":"invalid_request","message":"invalid request"}})
}

#[tokio::test]
async fn malformed_requests_and_reused_ids_leave_valid_shutdown_available() {
    let cancelled = json!({"id":1,"result":{"cancelled":false}});
    for (name, input, expected) in [
        ("malformed JSON", "{broken}\n", vec![rejected(Value::Null)]),
        (
            "negative ID",
            "{\"id\":-1,\"method\":\"cancel\",\"params\":{}}\n",
            vec![rejected(Value::Null)],
        ),
        (
            "zero ID",
            "{\"id\":0,\"method\":\"cancel\",\"params\":{}}\n",
            vec![rejected(json!(0))],
        ),
        (
            "non-object parameters",
            "{\"id\":1,\"method\":\"cancel\",\"params\":[]}\n",
            vec![rejected(json!(1))],
        ),
        (
            "unknown request member",
            "{\"id\":1,\"method\":\"cancel\",\"params\":{},\"extra\":true}\n",
            vec![rejected(Value::Null)],
        ),
        (
            "reused ID",
            "{\"id\":1,\"method\":\"cancel\",\"params\":{}}\n{\"id\":1,\"method\":\"cancel\",\"params\":{}}\n",
            vec![cancelled, rejected(json!(1))],
        ),
    ] {
        let input = format!("{input}{{\"id\":99,\"method\":\"shutdown\",\"params\":{{}}}}\n");
        let (frames, sent) = exchange(input.as_bytes()).await;
        sent.expect("write requests and shutdown");
        let mut expected = expected;
        expected.push(json!({"id":99,"result":{}}));
        assert_eq!(frames, expected, "{name}");
    }
}

#[tokio::test]
async fn input_frame_limit_accepts_exact_boundary_and_closes_on_one_byte_over() {
    for size in [FRAME_LIMIT, FRAME_LIMIT + 1] {
        let mut input = b"{\"id\":1,\"method\":\"cancel\",\"params\":{}}".to_vec();
        input.resize(size, b' ');
        input.extend_from_slice(b"\n{\"id\":2,\"method\":\"shutdown\",\"params\":{}}\n");
        let (frames, sent) = exchange(&input).await;
        if size == FRAME_LIMIT {
            sent.expect("write boundary frame and shutdown");
            assert_eq!(
                frames,
                vec![
                    json!({"id":1,"result":{"cancelled":false}}),
                    json!({"id":2,"result":{}}),
                ]
            );
        } else {
            if let Err(error) = sent {
                assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
            }
            assert_eq!(frames, vec![rejected(Value::Null)]);
        }
    }
}
