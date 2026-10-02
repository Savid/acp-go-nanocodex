"""Build, verify and describe release archives for the adapter and its helper.

Every subcommand works from the repository root and uses only the Python
standard library, git, the pinned Go and Rust toolchains, and the pinned
cargo-zigbuild and zig supplied by the Makefile.
"""

import argparse
import gzip
import hashlib
import http.client
import io
import json
import os
import platform
import random
import re
import shutil
import struct
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
TAG_PATTERN = re.compile(r"v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(-[0-9A-Za-z.-]+)?")
TARGETS = {
    "linux_amd64": {"rust": "x86_64-unknown-linux-gnu", "goos": "linux", "goarch": "amd64", "host": ("Linux", "x86_64")},
    "linux_arm64": {"rust": "aarch64-unknown-linux-gnu", "goos": "linux", "goarch": "arm64", "host": ("Linux", "aarch64")},
    "darwin_arm64": {"rust": "aarch64-apple-darwin", "goos": "darwin", "goarch": "arm64", "host": ("Darwin", "arm64")},
}
SHARED = ("version", "commit", "helperVersion", "helperFingerprint", "rustc", "go")
PROBE_TIMEOUT = 60
ZIG_MIRRORS = "https://ziglang.org/download/community-mirrors.txt"
ZIG_SOURCE = "acp-go-nanocodex-release"
LC_BUILD_VERSION = 0x32


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


def required_helper(tag):
    """Return the helper release and fingerprint the adapter at HEAD requires, refusing a release other than tag."""
    required = json.loads(run(["go", "run", f"./cmd/{COMMAND}", "-nanocodex-helper-release"], env=go_environment()))
    if required.get("helperVersion") != require_tag(tag):
        raise ReleaseError(f"HelperVersion is {required.get('helperVersion')}; tag {tag} requires {require_tag(tag)}")
    return required


def default_branch():
    try:
        return run(["git", "symbolic-ref", "--short", "refs/remotes/origin/HEAD"])
    except ReleaseError as error:
        raise ReleaseError("origin/HEAD is unset; pass RELEASE_BRANCH=origin/<default branch>") from error


def check(tag, branch):
    """Refuse a tag that does not name this clean commit's helper release."""
    release = require_tag(tag)
    for source, version in (("native/Cargo.toml", cargo_version()), ("native/Cargo.lock", lock_version())):
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
    required = required_helper(tag)
    print(f"release {tag} commit {head} helper {release} fingerprint {required['helperFingerprint']}", flush=True)


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


def require_identity(state, required, where):
    reported = {key: state.get(key) for key in required}
    if reported != required:
        raise ReleaseError(f"{where} reports {reported}; the adapter requires {required}")


def verify_executables(directory, tag, required):
    """Require the command and helper in directory to identify this release."""
    command, helper = directory / COMMAND, directory / HELPER
    reported_version = run([str(command), "-version"])
    if reported_version != tag:
        raise ReleaseError(f"{command} -version reports {reported_version!r}, not {tag}")
    require_identity(json.loads(run([str(command), "-nanocodex-helper-release"])), required, str(command))
    require_identity(probe_local(helper), required, str(helper))
    print(f"{directory.name}: command {tag}, helper {required['helperVersion']}, fingerprint {required['helperFingerprint']}", flush=True)


def host_target():
    host = (platform.system(), platform.machine())
    return next((name for name, spec in TARGETS.items() if spec["host"] == host), None)


def version_tuple(text):
    return (tuple(map(int, text.split("."))) + (0, 0))[:3]


def glibc_requirement(path):
    versions = {tuple(map(int, match.groups(default="0"))) for match in re.finditer(rb"GLIBC_(\d+)\.(\d+)(?:\.(\d+))?\x00", path.read_bytes())}
    return max(versions, default=(0, 0, 0))


def macos_requirement(path):
    """Return the minimum macOS version recorded in a 64-bit Mach-O executable."""
    data = path.read_bytes()
    magic, _, _, _, commands, _, _, _ = struct.unpack_from("<8I", data)
    if magic != 0xFEEDFACF:
        raise ReleaseError(f"{path} is not a 64-bit little-endian Mach-O executable")
    offset = 32
    for _ in range(commands):
        command, size = struct.unpack_from("<2I", data, offset)
        if command == LC_BUILD_VERSION:
            minimum = struct.unpack_from("<I", data, offset + 12)[0]
            return (minimum >> 16, (minimum >> 8) & 0xFF, minimum & 0xFF)
        offset += size
    raise ReleaseError(f"{path} has no LC_BUILD_VERSION load command")


def native_toolchain(goos, zig_dir, cargo_zigbuild):
    """Describe the C toolchain and linker that build a target's helper."""
    if goos == "linux":
        if not (zig_dir and cargo_zigbuild):
            raise ReleaseError("Linux targets require zig and cargo-zigbuild")
        return {"zig": run([str(Path(zig_dir) / "zig"), "version"]), "cargoZigbuild": run([cargo_zigbuild, "--version"])}
    return {"clang": run(["xcrun", "clang", "--version"]).splitlines()[0], "macosSdk": run(["xcrun", "--show-sdk-version"])}


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


def build(tag, targets, glibc_floor, macos_floor, zig_dir, cargo_zigbuild):
    """Build reproducible archives for each target under dist/."""
    unknown = [target for target in targets if target not in TARGETS]
    if unknown or not targets:
        raise ReleaseError(f"unknown release targets {unknown or targets}; choose from {sorted(TARGETS)}")
    head, required = commit(), required_helper(tag)
    epoch = int(run(["git", "log", "-1", "--format=%ct", head]))
    cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo")).absolute()
    remaps = {ROOT: "/src", cargo_home: "/cargo", cargo_home.resolve(): "/cargo"}
    rust_env = dict(
        os.environ,
        SOURCE_DATE_EPOCH=str(epoch),
        CARGO_TARGET_DIR=str(WORK / "cargo"),
        CARGO_PROFILE_RELEASE_STRIP="symbols",
        CARGO_INCREMENTAL="0",
        MACOSX_DEPLOYMENT_TARGET=macos_floor,
        RUSTFLAGS=" ".join(f"--remap-path-prefix={source}={target}" for source, target in remaps.items()),
        CFLAGS=" ".join(f"-ffile-prefix-map={source}={target}" for source, target in remaps.items()),
    )
    if zig_dir:
        rust_env["PATH"] = f"{zig_dir}{os.pathsep}{rust_env['PATH']}"
    floors = {"linux": ("glibcFloor", glibc_floor, glibc_requirement), "darwin": ("macosFloor", macos_floor, macos_requirement)}
    rustc = run(["rustc", "-V"], cwd=ROOT / "native")
    go_version = run(["go", "env", "GOVERSION"], env=go_environment())
    for target in targets:
        spec = TARGETS[target]
        triple = spec["rust"]
        toolchain = native_toolchain(spec["goos"], zig_dir, cargo_zigbuild)
        run(["rustup", "target", "add", triple], cwd=ROOT / "native")
        if spec["goos"] == "linux":
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
            ["go", "build", "-trimpath", "-ldflags", f"-s -w -X main.buildVersion={tag}", "-o", str(command), f"./cmd/{COMMAND}"],
            env=go_environment(CGO_ENABLED="0", GOOS=spec["goos"], GOARCH=spec["goarch"]),
        )
        command.chmod(0o755)
        floor_key, floor, requirement = floors[spec["goos"]]
        for executable in (helper, command):
            needed = requirement(executable)
            if needed > version_tuple(floor):
                raise ReleaseError(f"{executable} requires {spec['goos']} {'.'.join(map(str, needed))}, above the {floor} floor")
        if target == host_target():
            verify_executables(out, tag, required)
        archive = DIST / archive_name(tag, target)
        write_archive(archive, [(COMMAND, command, 0o755), (HELPER, helper, 0o755), ("LICENSE", ROOT / "LICENSE", 0o644)], epoch)
        record = {
            "version": tag,
            "commit": head,
            **required,
            "rustc": rustc,
            "go": go_version,
            floor_key: floor,
            **toolchain,
            "archive": archive.name,
            "archiveSha256": sha256_file(archive),
            "helperSha256": sha256_file(helper),
            "commandSha256": sha256_file(command),
        }
        (out / "build.json").write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
        print(f"{archive.relative_to(ROOT)} {record['archiveSha256']}", flush=True)


def load_record(target):
    path = DIST / target / "build.json"
    if not path.is_file():
        raise ReleaseError(f"{path.relative_to(ROOT)} is missing; build {target} first")
    return json.loads(path.read_text())


def smoke(tag, target, image):
    """Verify a built archive from its own contents, then run integration smoke."""
    record = load_record(target)
    required = required_helper(tag)
    expected = {"version": tag, "commit": commit(), **required}
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
        bundle.extractall(directory, filter="data")
    for name, key in ((COMMAND, "commandSha256"), (HELPER, "helperSha256")):
        if sha256_file(directory / name) != record[key]:
            raise ReleaseError(f"{archive.name}:{name} digest differs from its build record")
    verify_executables(directory, tag, required)
    if image:
        require_identity(probe_image(directory / HELPER, image), required, f"{HELPER} in {image}")
        print(f"{target}: helper initializes in {image}", flush=True)
    env = go_environment(
        ACP_GO_NANOCODEX_RUN_INTEGRATION="1",
        ACP_GO_NANOCODEX_RUN_LIVE_TOKENS="0",
        ACP_GO_NANOCODEX_AGENT_BINARY=str(directory / COMMAND),
        ACP_GO_NANOCODEX_HARNESS_PATH=str(directory / HELPER),
    )
    run(["go", "test", "-race", "-count=1", "-tags=integration", "-timeout=300s", "-v", "./integration/..."], env=env, capture=False)


def manifest(tag):
    """Write release-manifest.json and SHA256SUMS for every target built under dist/."""
    expected = {"version": tag, "commit": commit(), "helperVersion": require_tag(tag)}
    records = {path.parent.name: json.loads(path.read_text()) for path in sorted(DIST.glob("*/build.json"))}
    unknown = sorted(set(records) - set(TARGETS))
    if unknown or not records:
        raise ReleaseError(f"dist/ holds build records for {sorted(records)}; targets are {sorted(TARGETS)}")
    first = next(iter(records.values()))
    mismatched = {key: first[key] for key, value in expected.items() if first[key] != value}
    if mismatched:
        raise ReleaseError(f"build records differ from this checkout: {mismatched}")
    for target, record in records.items():
        disagreeing = [key for key in SHARED if record[key] != first[key]]
        if disagreeing:
            raise ReleaseError(f"{target} build record disagrees on {disagreeing}")
        for key, path in (("archiveSha256", DIST / record["archive"]), ("helperSha256", DIST / target / HELPER), ("commandSha256", DIST / target / COMMAND)):
            if sha256_file(path) != record[key]:
                raise ReleaseError(f"{path.relative_to(ROOT)} digest differs from its build record")
    document = {
        "module": MODULE,
        **{key: first[key] for key in SHARED},
        "targets": {target: {key: value for key, value in record.items() if key not in SHARED} for target, record in records.items()},
    }
    (DIST / "release-manifest.json").write_text(json.dumps(document, indent=2) + "\n")
    sums = sorted([record["archive"] for record in records.values()] + ["release-manifest.json"])
    (DIST / "SHA256SUMS").write_text("".join(f"{sha256_file(DIST / name)}  {name}\n" for name in sums))
    print((DIST / "SHA256SUMS").read_text(), end="")


def zig_sources(version, filename):
    """Yield zig tarball URLs: community mirrors in random order, then ziglang.org."""
    try:
        with urllib.request.urlopen(ZIG_MIRRORS, timeout=30) as response:
            mirrors = [mirror for mirror in response.read().decode().split() if mirror.startswith("https://")]
    except (OSError, http.client.HTTPException) as error:
        print(f"zig mirror list unavailable: {error}", file=sys.stderr)
        mirrors = []
    random.shuffle(mirrors)
    yield from (f"{mirror}/{filename}?source={ZIG_SOURCE}" for mirror in mirrors)
    yield f"https://ziglang.org/download/{version}/{filename}"


def install_zig(version, root, sums):
    """Install the pinned zig release for this host.

    The pinned digest fixes the tarball bytes, so any mirror is as trustworthy
    as ziglang.org.
    """
    machine = {"arm64": "aarch64"}.get(platform.machine(), platform.machine())
    system = {"Darwin": "macos"}.get(platform.system(), platform.system().lower())
    host = f"{machine}-{system}"
    pinned = dict(entry.split("=", 1) for entry in sums)
    if host not in pinned:
        raise ReleaseError(f"no pinned zig {version} digest for {host}")
    name = f"zig-{host}-{version}"
    with tempfile.TemporaryDirectory(prefix="zig-") as scratch:
        tarball = Path(scratch) / f"{name}.tar.xz"
        for url in zig_sources(version, tarball.name):
            try:
                with urllib.request.urlopen(url, timeout=300) as response, open(tarball, "wb") as file:
                    shutil.copyfileobj(response, file)
            except (OSError, http.client.HTTPException) as error:
                print(f"zig download from {url} failed: {error}", file=sys.stderr)
                continue
            if sha256_file(tarball) == pinned[host]:
                break
            print(f"zig download from {url} differs from the pinned digest", file=sys.stderr)
        else:
            raise ReleaseError(f"no source served {tarball.name} with digest {pinned[host]}")
        with tarfile.open(tarball) as bundle:
            bundle.extractall(scratch, filter="data")
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
    commands.choices["build"].add_argument("--macos-floor", required=True)
    commands.choices["build"].add_argument("--zig-dir", default="")
    commands.choices["build"].add_argument("--cargo-zigbuild", default="")
    commands.choices["smoke"].add_argument("--target", required=True)
    commands.choices["smoke"].add_argument("--image", default="")
    zig = commands.add_parser("zig")
    zig.add_argument("--version", required=True)
    zig.add_argument("--root", required=True)
    zig.add_argument("--sha256", action="append", default=[], metavar="HOST=DIGEST")
    args = parser.parse_args()
    try:
        if args.command == "check":
            check(args.tag, args.branch)
        elif args.command == "build":
            build(args.tag, args.targets.split(), args.glibc_floor, args.macos_floor, args.zig_dir, args.cargo_zigbuild)
        elif args.command == "smoke":
            smoke(args.tag, args.target, args.image)
        elif args.command == "manifest":
            manifest(args.tag)
        else:
            install_zig(args.version, args.root, args.sha256)
    except ReleaseError as error:
        print(f"release: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
