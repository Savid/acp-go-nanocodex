"""Build, verify and describe release archives for the adapter and its helper.

Every subcommand works from the repository root and uses only the Python
standard library, git, the pinned Go and Rust toolchains, and the pinned
cargo-zigbuild and zig supplied by the Makefile.
"""

import argparse
import gzip
import hashlib
import io
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DIST = ROOT / "dist"
WORK = ROOT / ".tmp" / "release"
MODULE = "github.com/savid/acp-go-nanocodex"
COMMAND = "acp-go-nanocodex"
HELPER = "acp-go-nanocodex-native"
PROTOCOL_VERSION = 1
TAG_PATTERN = re.compile(r"v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(-[0-9A-Za-z.-]+)?")
FINGERPRINT_FILES = (
    "native/Cargo.toml",
    "native/Cargo.lock",
    "native/rust-toolchain.toml",
    "native/build.rs",
    "internal/nanocodex/protocol.go",
    "internal/nanocodex/client.go",
)
TARGETS = {
    "linux_amd64": {"rust": "x86_64-unknown-linux-gnu", "goos": "linux", "goarch": "amd64", "host": ("Linux", "x86_64")},
    "linux_arm64": {"rust": "aarch64-unknown-linux-gnu", "goos": "linux", "goarch": "arm64", "host": ("Linux", "aarch64")},
    "darwin_arm64": {"rust": "aarch64-apple-darwin", "goos": "darwin", "goarch": "arm64", "host": ("Darwin", "arm64")},
}
PROBE_TIMEOUT = 60


class ReleaseError(Exception):
    """A release precondition or verification failed."""


def run(args, *, cwd=ROOT, env=None, capture=True):
    result = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=capture, check=False)
    if result.returncode != 0:
        detail = (result.stderr or "").strip() if capture else ""
        raise ReleaseError(f"{' '.join(map(str, args))} exited {result.returncode}" + (f": {detail}" if detail else ""))
    return result.stdout.strip() if capture else ""


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_text(relative):
    return (ROOT / relative).read_text()


def helper_version():
    match = re.search(r'^const HelperVersion = "([^"]+)"$', read_text("internal/nanocodex/protocol.go"), re.MULTILINE)
    if match is None:
        raise ReleaseError("internal/nanocodex/protocol.go declares no HelperVersion")
    return match[1]


def cargo_version():
    match = re.search(r'^\[package\]\n(?:[^\[]*\n)*?version = "([^"]+)"$', read_text("native/Cargo.toml"), re.MULTILINE)
    if match is None:
        raise ReleaseError("native/Cargo.toml declares no package version")
    return match[1]


def lock_version():
    match = re.search(rf'^\[\[package\]\]\nname = "{HELPER}"\nversion = "([^"]+)"$', read_text("native/Cargo.lock"), re.MULTILINE)
    if match is None:
        raise ReleaseError(f"native/Cargo.lock has no {HELPER} entry")
    return match[1]


def source_fingerprint():
    """Hash the helper build inputs exactly as native/build.rs does."""
    sources = [f"native/src/{path.name}" for path in (ROOT / "native/src").iterdir() if path.is_file() and path.suffix == ".rs"]
    digest = hashlib.sha256()
    for relative in sorted([*FINGERPRINT_FILES, *sources]):
        digest.update(relative.encode() + b"\0" + (ROOT / relative).read_bytes() + b"\0")
    return digest.hexdigest()


def go_environment(**overrides):
    match = re.search(r"^go (\d+\.\d+\.\d+)$", read_text("go.mod"), re.MULTILINE)
    if match is None:
        raise ReleaseError("go.mod must declare an exact go version")
    env = dict(os.environ, GOTOOLCHAIN=f"go{match[1]}", GOFLAGS="-mod=readonly", GOWORK="off")
    env.update(overrides)
    return env


def commit():
    return run(["git", "rev-parse", "HEAD"])


def require_tag(tag):
    if not TAG_PATTERN.fullmatch(tag or ""):
        raise ReleaseError(f"tag {tag!r} is not v<major>.<minor>.<patch>[-<prerelease>]")
    return tag[1:]


def default_branch():
    try:
        return run(["git", "symbolic-ref", "--short", "refs/remotes/origin/HEAD"])
    except ReleaseError as error:
        raise ReleaseError("origin/HEAD is unset; pass RELEASE_BRANCH=origin/<default branch>") from error


def check(tag, branch):
    """Refuse a tag that does not name this clean commit's helper release."""
    release = require_tag(tag)
    versions = {"HelperVersion": helper_version(), "native/Cargo.toml": cargo_version(), "native/Cargo.lock": lock_version()}
    for source, version in versions.items():
        if version != release:
            raise ReleaseError(f"{source} is {version}; tag {tag} requires {release}")
    if run(["git", "status", "--porcelain"]):
        raise ReleaseError("working tree has uncommitted or untracked files")
    head = commit()
    tagged = subprocess.run(["git", "rev-parse", "--verify", "--quiet", f"refs/tags/{tag}^{{commit}}"], cwd=ROOT, text=True, capture_output=True, check=False)
    if tagged.returncode == 0 and tagged.stdout.strip() != head:
        raise ReleaseError(f"tag {tag} points at {tagged.stdout.strip()}, not HEAD {head}")
    branch = branch or default_branch()
    if subprocess.run(["git", "merge-base", "--is-ancestor", head, branch], cwd=ROOT, check=False).returncode != 0:
        raise ReleaseError(f"HEAD {head} is not on {branch}")
    run(["go", "mod", "verify"], env=go_environment())
    fingerprint = source_fingerprint()
    adapter = json.loads(run(["go", "run", "./cmd/acp-go-nanocodex", "-nanocodex-helper-release"], env=go_environment()))
    if adapter != {"helperVersion": release, "helperFingerprint": fingerprint}:
        raise ReleaseError(f"Go adapter reports {adapter}; source requires {release} {fingerprint}")
    print(f"release {tag} commit {head} helper {release} fingerprint {fingerprint}", flush=True)


def uuid7():
    value = (int(time.time() * 1000) << 80) | int.from_bytes(os.urandom(10), "big")
    value = (value & ~(0xF << 76)) | (0x7 << 76)
    value = (value & ~(0x3 << 62)) | (0x2 << 62)
    text = f"{value:032x}"
    return f"{text[:8]}-{text[8:12]}-{text[12:16]}-{text[16:20]}-{text[20:]}"


def probe_helper(argv, env=None, cwd=None):
    """Initialize and shut down a helper; return its initialization result."""
    with tempfile.TemporaryFile(mode="w+") as stderr:
        process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=stderr, text=True, env=env, cwd=cwd)
        timer = threading.Timer(PROBE_TIMEOUT, process.kill)
        timer.start()

        def call(request_id, method, params):
            process.stdin.write(json.dumps({"id": request_id, "method": method, "params": params}) + "\n")
            process.stdin.flush()
            for line in process.stdout:
                frame = json.loads(line)
                if frame.get("id") != request_id:
                    continue
                if "error" in frame:
                    raise ReleaseError(f"helper {method} failed: {frame['error'].get('code')}: {frame['error'].get('message')}")
                return frame["result"]
            raise ReleaseError(f"helper exited before replying to {method}")

        try:
            state = call(1, "initialize", {"sessionId": uuid7(), "apiBaseUrl": "http://127.0.0.1:9/v1"})
            call(2, "shutdown", {})
            process.stdin.close()
            if process.wait() != 0:
                raise ReleaseError(f"helper exited {process.returncode} after shutdown")
            return state
        except (OSError, ValueError, KeyError) as error:
            raise ReleaseError(f"helper probe failed: {error}") from error
        finally:
            timer.cancel()
            if process.poll() is None:
                process.kill()
            process.wait()
            stderr.seek(0)
            diagnostics = stderr.read().strip()
            if process.returncode != 0 and diagnostics:
                print(diagnostics[-4000:], file=sys.stderr)


def probe_local(helper):
    with tempfile.TemporaryDirectory(prefix="nanocodex-probe-") as home:
        env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": home, "CODEX_HOME": home, "OPENAI_API_KEY": "probe"}
        return probe_helper([str(helper)], env=env, cwd=home)


def probe_image(helper, image):
    """Probe the helper inside a container image without network access."""
    command = [
        "docker",
        "run",
        "--rm",
        "-i",
        "--network",
        "none",
        "-v",
        f"{helper.parent}:/release:ro",
        "-e",
        "HOME=/tmp/probe",
        "-e",
        "CODEX_HOME=/tmp/probe",
        "-e",
        "OPENAI_API_KEY=probe",
        image,
        "sh",
        "-c",
        f"mkdir -p /tmp/probe && cd /tmp/probe && exec /release/{helper.name}",
    ]
    return probe_helper(command)


def require_identity(state, release, fingerprint, where):
    reported = {key: state.get(key) for key in ("protocolVersion", "helperVersion", "helperFingerprint")}
    expected = {"protocolVersion": PROTOCOL_VERSION, "helperVersion": release, "helperFingerprint": fingerprint}
    if reported != expected:
        raise ReleaseError(f"{where} reports {reported}; expected {expected}")


def verify_executables(directory, tag, fingerprint):
    """Require the command and helper in directory to identify this release."""
    release = tag[1:]
    command, helper = directory / COMMAND, directory / HELPER
    reported_version = run([str(command), "-version"])
    if reported_version != tag:
        raise ReleaseError(f"{command} -version reports {reported_version!r}, not {tag}")
    adapter = json.loads(run([str(command), "-nanocodex-helper-release"]))
    if adapter != {"helperVersion": release, "helperFingerprint": fingerprint}:
        raise ReleaseError(f"{command} requires {adapter}; source is {release} {fingerprint}")
    require_identity(probe_local(helper), release, fingerprint, str(helper))
    print(f"{directory.name}: command {tag}, helper {release}, fingerprint {fingerprint}", flush=True)


def host_target():
    machine = {"amd64": "x86_64", "arm64": "aarch64" if platform.system() == "Linux" else "arm64"}.get(platform.machine().lower(), platform.machine())
    return next((name for name, spec in TARGETS.items() if spec["host"] == (platform.system(), machine)), None)


def glibc_requirement(path):
    versions = {tuple(map(int, match.groups(default="0"))) for match in re.finditer(rb"GLIBC_(\d+)\.(\d+)(?:\.(\d+))?\x00", path.read_bytes())}
    return max(versions, default=(0, 0, 0))


def extract(bundle, directory):
    """Extract a tar archive, refusing unsafe members where Python supports it."""
    if hasattr(tarfile, "data_filter"):
        bundle.extractall(directory, filter="data")
        return
    for member in bundle.getmembers():
        if not (member.isfile() or member.isdir()) or member.name.startswith("/") or ".." in Path(member.name).parts:
            raise ReleaseError(f"refusing archive member {member.name}")
    bundle.extractall(directory)


def archive_name(tag, target):
    return f"{COMMAND}_{tag}_{target}.tar.gz"


def write_archive(path, members, epoch):
    """Write a byte-stable gzip tar of (name, source, mode) members."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, source, mode in sorted(members):
            info = tarfile.TarInfo(name)
            info.size, info.mtime, info.mode = source.stat().st_size, epoch, mode
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            with open(source, "rb") as file:
                archive.addfile(info, file)
    with open(path, "wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0) as compressed:
        compressed.write(buffer.getvalue())


def build(tag, targets, glibc_floor, zig_dir, cargo_zigbuild):
    """Build reproducible archives for each target under dist/."""
    release = require_tag(tag)
    unknown = [target for target in targets if target not in TARGETS]
    if unknown or not targets:
        raise ReleaseError(f"unknown release targets {unknown or targets}; choose from {sorted(TARGETS)}")
    head, fingerprint = commit(), source_fingerprint()
    epoch = int(run(["git", "log", "-1", "--format=%ct", head]))
    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo")).resolve()
    remaps = {ROOT: "/src", cargo_home: "/cargo"}
    rust_env = dict(
        os.environ,
        SOURCE_DATE_EPOCH=str(epoch),
        CARGO_TARGET_DIR=str(WORK / "cargo"),
        CARGO_PROFILE_RELEASE_STRIP="symbols",
        CARGO_INCREMENTAL="0",
        RUSTFLAGS=" ".join(f"--remap-path-prefix={source}={target}" for source, target in remaps.items()),
        CFLAGS=" ".join(f"-ffile-prefix-map={source}={target}" for source, target in remaps.items()),
    )
    if zig_dir:
        rust_env["PATH"] = f"{zig_dir}{os.pathsep}{rust_env['PATH']}"
    rustc = run(["rustc", "-V"], cwd=ROOT / "native")
    go_version = run(["go", "env", "GOVERSION"], env=go_environment())
    for target in targets:
        spec = TARGETS[target]
        triple = spec["rust"]
        run(["rustup", "target", "add", triple], cwd=ROOT / "native")
        if spec["goos"] == "linux":
            if not cargo_zigbuild:
                raise ReleaseError("Linux targets require cargo-zigbuild")
            cargo = [cargo_zigbuild, "zigbuild", "--target", f"{triple}.{glibc_floor}"]
        else:
            cargo = ["cargo", "build", "--target", triple]
        print(f"building {target} helper", flush=True)
        run([*cargo, "--locked", "--release"], cwd=ROOT / "native", env=rust_env, capture=False)
        out = DIST / target
        shutil.rmtree(out, ignore_errors=True)
        out.mkdir(parents=True)
        helper = out / HELPER
        shutil.copyfile(WORK / "cargo" / triple / "release" / HELPER, helper)
        helper.chmod(0o755)
        print(f"building {target} command", flush=True)
        command = out / COMMAND
        run(
            ["go", "build", "-trimpath", "-ldflags", f"-s -w -X main.buildVersion={tag}", "-o", str(command), "./cmd/acp-go-nanocodex"],
            env=go_environment(CGO_ENABLED="0", GOOS=spec["goos"], GOARCH=spec["goarch"]),
        )
        command.chmod(0o755)
        record = {}
        if spec["goos"] == "linux":
            floor = tuple(map(int, glibc_floor.split("."))) + (0,)
            required = glibc_requirement(helper)
            if required > floor[:3]:
                raise ReleaseError(f"{helper} requires GLIBC_{'.'.join(map(str, required))}, above the {glibc_floor} floor")
            record["glibcFloor"] = glibc_floor
        if target == host_target():
            verify_executables(out, tag, fingerprint)
        archive = DIST / archive_name(tag, target)
        write_archive(archive, [(COMMAND, command, 0o755), (HELPER, helper, 0o755), ("LICENSE", ROOT / "LICENSE", 0o644)], epoch)
        record.update(
            target=target,
            version=tag,
            commit=head,
            helperVersion=release,
            helperFingerprint=fingerprint,
            rustc=rustc,
            go=go_version,
            archive=archive.name,
            archiveSha256=sha256_file(archive),
            helperSha256=sha256_file(helper),
            commandSha256=sha256_file(command),
        )
        (out / "build.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
        print(f"{archive.relative_to(ROOT)} {record['archiveSha256']}", flush=True)


def load_record(target):
    path = DIST / target / "build.json"
    if not path.is_file():
        raise ReleaseError(f"{path.relative_to(ROOT)} is missing; build {target} first")
    return json.loads(path.read_text())


def smoke(tag, target, image):
    """Verify a built archive from its own contents, then run integration smoke."""
    release = require_tag(tag)
    record = load_record(target)
    fingerprint = source_fingerprint()
    expected = {"version": tag, "commit": commit(), "helperVersion": release, "helperFingerprint": fingerprint}
    mismatched = {key: record.get(key) for key, value in expected.items() if record.get(key) != value}
    if mismatched:
        raise ReleaseError(f"{target} build record differs from this checkout: {mismatched}")
    archive = DIST / record["archive"]
    if sha256_file(archive) != record["archiveSha256"]:
        raise ReleaseError(f"{archive.name} digest differs from its build record")
    directory = WORK / "smoke" / target
    shutil.rmtree(directory, ignore_errors=True)
    directory.mkdir(parents=True)
    with tarfile.open(archive) as bundle:
        names = sorted(bundle.getnames())
        if names != sorted([COMMAND, HELPER, "LICENSE"]):
            raise ReleaseError(f"{archive.name} contains {names}")
        extract(bundle, directory)
    for name, key in ((COMMAND, "commandSha256"), (HELPER, "helperSha256")):
        if sha256_file(directory / name) != record[key]:
            raise ReleaseError(f"{archive.name}:{name} digest differs from its build record")
    verify_executables(directory, tag, fingerprint)
    if image:
        require_identity(probe_image(directory / HELPER, image), release, fingerprint, f"{HELPER} in {image}")
        print(f"{target}: helper initializes in {image}", flush=True)
    env = go_environment(
        ACP_GO_NANOCODEX_RUN_INTEGRATION="1",
        ACP_GO_NANOCODEX_RUN_LIVE_TOKENS="0",
        ACP_GO_NANOCODEX_AGENT_BINARY=str(directory / COMMAND),
        ACP_GO_NANOCODEX_HARNESS_PATH=str(directory / HELPER),
    )
    run(["go", "test", "-race", "-count=1", "-tags=integration", "-timeout=300s", "-v", "./integration/..."], env=env, capture=False)


def manifest(tag, targets):
    """Write release-manifest.json and SHA256SUMS from the built targets."""
    release = require_tag(tag)
    records = {target: load_record(target) for target in targets}
    shared = ("version", "commit", "helperVersion", "helperFingerprint", "rustc", "go")
    expected = {"version": tag, "commit": commit(), "helperVersion": release, "helperFingerprint": source_fingerprint()}
    for target, record in records.items():
        for key in shared:
            if record[key] != records[targets[0]][key] or (key in expected and record[key] != expected[key]):
                raise ReleaseError(f"{target} {key} {record[key]!r} disagrees with the release")
        for key, path in (("archiveSha256", DIST / record["archive"]), ("helperSha256", DIST / target / HELPER), ("commandSha256", DIST / target / COMMAND)):
            if sha256_file(path) != record[key]:
                raise ReleaseError(f"{path.relative_to(ROOT)} digest differs from its build record")
    first = records[targets[0]]
    document = {
        "module": MODULE,
        "version": tag,
        "commit": first["commit"],
        "helperVersion": release,
        "protocolVersion": PROTOCOL_VERSION,
        "helperFingerprint": first["helperFingerprint"],
        "rustc": first["rustc"],
        "go": first["go"],
        "targets": {
            target: {key: record[key] for key in ("archive", "archiveSha256", "helperSha256", "commandSha256", "glibcFloor") if key in record}
            for target, record in sorted(records.items())
        },
    }
    (DIST / "release-manifest.json").write_text(json.dumps(document, indent=2) + "\n")
    sums = sorted([record["archive"] for record in records.values()] + ["release-manifest.json"])
    (DIST / "SHA256SUMS").write_text("".join(f"{sha256_file(DIST / name)}  {name}\n" for name in sums))
    print((DIST / "SHA256SUMS").read_text(), end="")


def install_zig(version, root, sums):
    """Install the pinned zig release for this host after checking its digest."""
    machine = {"amd64": "x86_64", "arm64": "aarch64"}.get(platform.machine().lower(), platform.machine().lower())
    system = "macos" if platform.system() == "Darwin" else platform.system().lower()
    host = f"{machine}-{system}"
    pinned = dict(entry.split("=", 1) for entry in sums)
    if host not in pinned:
        raise ReleaseError(f"no pinned zig {version} digest for {host}")
    name = f"zig-{host}-{version}"
    with tempfile.TemporaryDirectory(prefix="zig-") as scratch:
        tarball = Path(scratch) / f"{name}.tar.xz"
        with urllib.request.urlopen(f"https://ziglang.org/download/{version}/{name}.tar.xz", timeout=300) as response, open(tarball, "wb") as file:
            shutil.copyfileobj(response, file)
        if sha256_file(tarball) != pinned[host]:
            raise ReleaseError(f"{tarball.name} digest differs from the pinned {pinned[host]}")
        with tarfile.open(tarball) as bundle:
            extract(bundle, scratch)
        shutil.rmtree(root, ignore_errors=True)
        Path(root).parent.mkdir(parents=True, exist_ok=True)
        shutil.move(Path(scratch) / name, root)
    print(run([str(Path(root) / "zig"), "version"]))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("check", "build", "smoke", "manifest"):
        command = commands.add_parser(name)
        command.add_argument("--tag", required=True)
    commands.choices["check"].add_argument("--branch", default="")
    commands.choices["build"].add_argument("--targets", required=True)
    commands.choices["build"].add_argument("--glibc-floor", required=True)
    commands.choices["build"].add_argument("--zig-dir", default="")
    commands.choices["build"].add_argument("--cargo-zigbuild", default="")
    commands.choices["smoke"].add_argument("--target", required=True)
    commands.choices["smoke"].add_argument("--image", default="")
    commands.choices["manifest"].add_argument("--targets", required=True)
    zig = commands.add_parser("zig")
    zig.add_argument("--version", required=True)
    zig.add_argument("--root", required=True)
    zig.add_argument("--sha256", action="append", default=[], metavar="HOST=DIGEST")
    args = parser.parse_args()
    try:
        if args.command == "check":
            check(args.tag, args.branch)
        elif args.command == "build":
            build(args.tag, args.targets.split(), args.glibc_floor, args.zig_dir, args.cargo_zigbuild)
        elif args.command == "smoke":
            smoke(args.tag, args.target, args.image)
        elif args.command == "manifest":
            manifest(args.tag, args.targets.split())
        else:
            install_zig(args.version, args.root, args.sha256)
    except ReleaseError as error:
        print(f"release: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
