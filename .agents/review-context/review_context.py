# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Private review context for GreptimeDB PRs.

Used by the greptimedb-create-pr and greptimedb-review-pr skills. The author
side publishes rationale.md and agent sessions to the private review-sessions
repo; the reviewer side records a blind review before it may read them.

Requires Python 3.9+, git, gh (authenticated), and gitleaks for `publish`.
"""

import argparse
import gzip
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

UPSTREAM = "GreptimeTeam/greptimedb"
PRIVATE = "GreptimeTeam/review-sessions"
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]
GITLEAKS_CONFIG = SCRIPT_DIR / "gitleaks.toml"
BLIND_REVIEWER = REPO_ROOT / ".claude" / "agents" / "greptimedb-blind-reviewer.md"
REVIEW_CRITERIA = (
    REPO_ROOT / ".agents" / "skills" / "greptimedb-review-pr" / "references" / "review-criteria.md"
)
GH_CREDENTIAL_HELPER = "!gh auth git-credential"
BLIND_SECTIONS = ("## Findings", "## Reconstructed intent")
ISOLATION_MODES = ("claude-subagent", "codex-exec", "opencode-run", "none")
SESSION_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class Abort(Exception):
    pass


def run(cmd, *, cwd=None, env=None, check=True, timeout=600, binary=False):
    try:
        proc = subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=not binary,
            timeout=timeout,
        )
    except FileNotFoundError:
        raise Abort(f"command not found: {cmd[0]}")
    if check and proc.returncode != 0:
        stderr = proc.stderr if not binary else proc.stderr.decode(errors="replace")
        raise Abort(f"{' '.join(cmd[:4])} ... failed ({proc.returncode}): {stderr.strip()}")
    return proc


def gh_json(*args):
    return json.loads(run(["gh", *args]).stdout)


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def has_private_access():
    # GitHub answers 404 for a private repo the user cannot see. Any other
    # failure (expired login, rate limit, network) must not be mistaken for
    # "no access", or publish silently skips and review drops to blind-only.
    proc = run(["gh", "api", f"repos/{PRIVATE}"], check=False, timeout=60)
    if proc.returncode == 0:
        return True
    if "HTTP 404" in proc.stderr or "HTTP 403" in proc.stderr:
        return False
    raise Abort(f"cannot determine access to {PRIVATE}: {proc.stderr.strip()}")


def gh_login():
    return run(["gh", "api", "user", "-q", ".login"]).stdout.strip()


def pr_info(pr):
    return gh_json(
        "pr", "view", str(pr), "--repo", UPSTREAM,
        "--json", "number,title,body,headRefOid,url",
    )


def private_raw(path):
    """Return the file content at `path` in the private repo, or None if absent."""
    proc = run(
        ["gh", "api", "-H", "Accept: application/vnd.github.raw", f"repos/{PRIVATE}/contents/{path}"],
        check=False,
        binary=True,
    )
    if proc.returncode == 0:
        return proc.stdout
    if b"HTTP 404" in proc.stderr:
        return None
    raise Abort(f"reading {path} from {PRIVATE} failed: {proc.stderr.decode(errors='replace').strip()}")


def private_list(path):
    proc = run(["gh", "api", f"repos/{PRIVATE}/contents/{path}"], check=False)
    if proc.returncode != 0:
        if "HTTP 404" in proc.stderr:
            return []
        raise Abort(f"listing {path} in {PRIVATE} failed: {proc.stderr.strip()}")
    files = []
    for entry in json.loads(proc.stdout):
        if entry["type"] == "dir":
            files.extend(private_list(entry["path"]))
        else:
            files.append(entry["path"])
    return files


class PrivateCheckout:
    """A throwaway sparse clone of the private repo limited to one PR directory.

    Nothing is kept on disk afterwards: a lingering clone would let a blind
    reviewer read the rationale through plain file access.
    """

    def __init__(self, pr_dir):
        self.pr_dir = pr_dir
        self.tmp = Path(tempfile.mkdtemp(prefix="review-context-"))
        self.dir = self.tmp / "repo"

    def git(self, *args, check=True):
        return run(["git", *args], cwd=self.dir, check=check)

    def __enter__(self):
        url = f"https://github.com/{PRIVATE}.git"
        run([
            "git", "-c", "credential.helper=", "-c", f"credential.helper={GH_CREDENTIAL_HELPER}",
            "clone", "--quiet", "--depth", "1", "--filter=blob:none", "--no-checkout", url, str(self.dir),
        ])
        self.git("config", "--local", "credential.helper", "")
        self.git("config", "--local", "--add", "credential.helper", GH_CREDENTIAL_HELPER)
        self.git("config", "--local", "user.name", gh_login())
        self.git("config", "--local", "user.email", "noreply@greptime.com")
        self.branch = self._default_branch()
        if self.git("ls-remote", "--heads", "origin", check=False).stdout.strip() == "":
            self.git("checkout", "--quiet", "--orphan", self.branch)
        else:
            self.git("sparse-checkout", "set", self.pr_dir)
            self.git("checkout", "--quiet", self.branch)
        return self

    def __exit__(self, *exc):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _default_branch(self):
        ref = self.git("symbolic-ref", "--short", "refs/remotes/origin/HEAD", check=False).stdout.strip()
        return ref.split("/", 1)[1] if ref else "main"

    def path(self, rel):
        return self.dir / self.pr_dir / rel

    def commit_and_push(self, message, rels):
        paths = [f"{self.pr_dir}/{rel}" for rel in rels]
        for _ in range(3):
            self.git("add", "--", *paths)
            if self.git("diff", "--cached", "--quiet", check=False).returncode == 0:
                return None
            self.git("commit", "--quiet", "-m", message)
            if self.git("push", "--quiet", "origin", f"HEAD:refs/heads/{self.branch}", check=False).returncode == 0:
                return self.git("rev-parse", "HEAD").stdout.strip()
            # Someone pushed in between. Only the listed files are ours, so
            # rebuild the commit on the new tip instead of rebasing shallow history.
            self.git("fetch", "--quiet", "--depth", "1", "origin", self.branch)
            self.git("reset", "--quiet", "--mixed", "FETCH_HEAD")
        raise Abort(f"push to {PRIVATE} kept failing after 3 attempts")


# ---------------------------------------------------------------- author side


def locate_sessions(agent, session_id):
    """Return [(source, dest_name)] for one session, or raise if it is not on disk."""
    if not SESSION_ID_RE.match(session_id):
        raise Abort(f"invalid session id: {session_id!r}")
    home = Path.home()
    if agent == "claude":
        base = Path(os.environ.get("CLAUDE_CONFIG_DIR", home / ".claude")) / "projects"
        main = sorted(base.glob(f"*/{session_id}.jsonl"))
        if not main:
            raise Abort(f"Claude Code session {session_id} not found under {base}")
        found = [(main[0], f"claude-{session_id}.jsonl")]
        for sub in sorted(main[0].parent.glob(f"{session_id}/subagents/*.jsonl")):
            found.append((sub, f"claude-{session_id}/subagents/{sub.name}"))
        return found
    if agent == "codex":
        base = Path(os.environ.get("CODEX_HOME", home / ".codex"))
        hits = sorted(base.glob(f"sessions/**/rollout-*{session_id}.jsonl"))
        hits += sorted(base.glob(f"archived_sessions/rollout-*{session_id}.jsonl"))
        if not hits:
            raise Abort(f"Codex session {session_id} not found under {base}")
        return [(hits[0], f"codex-{session_id}.jsonl")]
    if agent == "opencode":
        return [(None, f"opencode-{session_id}.json")]
    raise Abort(f"unknown agent: {agent}")


def current_session_id(agent):
    var = {
        "claude": "CLAUDE_CODE_SESSION_ID",
        "codex": "CODEX_THREAD_ID",
        "opencode": "OPENCODE_SESSION_ID",
    }[agent]
    value = os.environ.get(var)
    if not value:
        raise Abort(f"{var} is not set; pass the session explicitly with --session {agent}:<id>")
    return value


def stage_sessions(specs, staging):
    names = []
    for agent, session_id in specs:
        for source, name in locate_sessions(agent, session_id):
            dest = staging / "sessions" / name
            dest.parent.mkdir(parents=True, exist_ok=True)
            if source is None:
                dest.write_bytes(run(["opencode", "export", session_id], binary=True).stdout)
            else:
                shutil.copyfile(source, dest)
            names.append(name)
    return names


# Records and fields with nothing a reviewer can read: UI state, hook output,
# token counters, duplicates of other records, and opaque signatures or
# encrypted reasoning. They are most of a raw session's size.
CLAUDE_DROP_TYPES = {
    "file-history-snapshot", "file-history-delta", "last-prompt", "mode", "permission-mode",
    "atis-latch", "ai-title", "cost-state", "queue-operation",
}
CLAUDE_DROP_ATTACHMENTS = {"hook_success", "total_tokens_reminder"}
CODEX_DROP_TYPES = {"event_msg", "world_state", "token_usage_record", "turn_context"}
CODEX_COMPACTED_KEEP = {"message", "window_number"}


def parse_time(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed if parsed.tzinfo else parsed.astimezone()


def before(stamp, since):
    try:
        return parse_time(stamp) < since
    except ValueError:
        return False


def compact_record(agent, record):
    """Return the record with reviewer-irrelevant parts removed, or None to drop it."""
    kind = record.get("type")
    if agent == "claude":
        if kind in CLAUDE_DROP_TYPES:
            return None
        if kind == "attachment" and (record.get("attachment") or {}).get("type") in CLAUDE_DROP_ATTACHMENTS:
            return None
        content = (record.get("message") or {}).get("content")
        if isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and block.get("type") == "thinking":
                    block.pop("signature", None)
    elif agent == "codex":
        if kind in CODEX_DROP_TYPES:
            return None
        payload = record.get("payload")
        if kind == "compacted" and isinstance(payload, dict):
            record["payload"] = {k: v for k, v in payload.items() if k in CODEX_COMPACTED_KEEP}
        if kind == "response_item" and isinstance(payload, dict) and payload.get("type") == "reasoning":
            payload.pop("encrypted_content", None)
    return record


def compact_session(path, agent, since):
    if agent == "opencode":
        if since:
            raise Abort("--since is not supported for OpenCode sessions; leave the session out instead")
        return
    kept = []
    with open(path, encoding="utf-8", errors="surrogateescape") as f:
        for line in f:
            try:
                record = json.loads(line)
            except ValueError:
                kept.append(line.rstrip("\n"))
                continue
            stamp = record.get("timestamp")
            if since and isinstance(stamp, str) and before(stamp, since):
                continue
            record = compact_record(agent, record)
            if record is not None:
                kept.append(json.dumps(record, ensure_ascii=False))
    path.write_text("\n".join(kept) + "\n", encoding="utf-8", errors="surrogateescape")


def truncate_strings(value, limit):
    if isinstance(value, str):
        if len(value) <= limit:
            return value
        head, tail = limit * 3 // 4, limit // 8
        return f"{value[:head]}\n...[truncated {len(value) - head - tail} chars]...\n{value[-tail:]}"
    if isinstance(value, list):
        return [truncate_strings(v, limit) for v in value]
    if isinstance(value, dict):
        return {k: truncate_strings(v, limit) for k, v in value.items()}
    return value


def truncate_session(path, limit):
    text = path.read_text(encoding="utf-8", errors="surrogateescape")
    if path.suffix == ".json":
        out = json.dumps(truncate_strings(json.loads(text), limit), ensure_ascii=False)
    else:
        lines = []
        for line in text.splitlines():
            try:
                lines.append(json.dumps(truncate_strings(json.loads(line), limit), ensure_ascii=False))
            except ValueError:
                lines.append(line[:limit])
        out = "\n".join(lines) + "\n"
    path.write_text(out, encoding="utf-8", errors="surrogateescape")


def gitleaks_findings(target):
    report = Path(tempfile.mkdtemp(prefix="gitleaks-")) / "report.json"
    try:
        run([
            "gitleaks", "dir", str(target), "--config", str(GITLEAKS_CONFIG),
            "--no-banner", "--exit-code", "0", "-f", "json", "-r", str(report),
        ])
        return json.loads(report.read_text(encoding="utf-8") or "[]")
    finally:
        shutil.rmtree(report.parent, ignore_errors=True)


def redact_once(staging, findings):
    by_file = {}
    for f in findings:
        if f["Secret"]:
            by_file.setdefault(Path(f["File"]), []).append(f)
    replaced = []
    for path, hits in by_file.items():
        path = (path if path.is_absolute() else staging / path).resolve()
        lines = path.read_text(encoding="utf-8", errors="surrogateescape").splitlines(keepends=True)
        for hit in hits:
            # Replace only inside the reported match on the reported lines. A
            # file-wide replace of a short secret such as `pass` would also
            # rewrite unrelated text and earlier markers.
            start, end = hit["StartLine"] - 1, hit["EndLine"]
            region = "".join(lines[start:end])
            if hit["Match"] not in region:
                # An identical hit on the same lines was already replaced. Any
                # match this misses is caught by the rescan in `redact`.
                continue
            masked = hit["Match"].replace(hit["Secret"], f"[REDACTED:{hit['RuleID']}]")
            lines[start:end] = [region.replace(hit["Match"], masked)] + [""] * (end - start - 1)
            replaced.append(f"{path.relative_to(staging)}:{hit['StartLine']} {hit['RuleID']}")
        path.write_text("".join(lines), encoding="utf-8", errors="surrogateescape")
    return replaced


def redact(staging):
    """Replace every gitleaks hit with a marker until a rescan comes back clean."""
    replaced = []
    findings = gitleaks_findings(staging)
    for _ in range(3):
        if not findings:
            return replaced
        replaced += redact_once(staging, findings)
        findings = gitleaks_findings(staging)
    if not findings:
        return replaced
    left = ", ".join(f"{Path(f['File']).name}:{f['StartLine']} {f['RuleID']}" for f in findings)
    raise Abort(f"gitleaks still reports findings after redaction, nothing pushed: {left}")


def split_front_matter(text):
    """Return ({key: raw value}, body) for a file written by `publish`."""
    if not text.startswith("---\n"):
        return {}, text
    head, _, body = text[4:].partition("\n---\n")
    front = dict(line.split(": ", 1) for line in head.splitlines() if ": " in line)
    return front, body.lstrip("\n")


def cmd_access(args):
    print("full" if has_private_access() else "none")


def cmd_publish(args):
    if not has_private_access():
        print(json.dumps({"status": "skipped", "reason": f"no access to {PRIVATE}"}))
        return
    if shutil.which("gitleaks") is None:
        raise Abort("gitleaks is not installed; refusing to publish unscanned sessions")
    source = Path(args.rationale or args.revision)
    if not source.is_file() or not source.read_text(encoding="utf-8").strip():
        raise Abort(f"file is missing or empty: {source}")

    specs = []
    if not args.no_current_session:
        specs.append((args.agent, current_session_id(args.agent)))
    for spec in args.session:
        agent, _, session_id = spec.partition(":")
        specs.append((agent, session_id))

    since = parse_time(args.since) if args.since else None
    info = pr_info(args.pr)
    pr_dir = f"{args.prefix}/{info['number']}"
    body, previous_sessions = source.read_text(encoding="utf-8"), []
    if args.revision:
        # Append, so a reviewer of an earlier round can read what changed in
        # the reasoning since then.
        existing = private_raw(f"{pr_dir}/rationale.md")
        if existing is None:
            raise Abort("no rationale is published for this PR yet; publish with --rationale first")
        front, previous = split_front_matter(existing.decode("utf-8"))
        previous_sessions = json.loads(front.get("sessions", "[]"))
        round_no = len(re.findall(r"^## Revision \d+", previous, re.M)) + 1
        body = (
            f"{previous.rstrip()}\n\n## Revision {round_no} ({now()[:10]}, head {info['headRefOid'][:7]})\n\n"
            f"{body.strip()}\n"
        )
    staging = Path(tempfile.mkdtemp(prefix="review-context-stage-")).resolve()
    try:
        names = stage_sessions(specs, staging)
        sizes = {}
        for name in names:
            path = staging / "sessions" / name
            sizes[name] = path.stat().st_size
            compact_session(path, name.split("-", 1)[0], since)
        all_sessions = previous_sessions + [n for n in names if n not in previous_sessions]
        header = (
            f"---\npr: {info['number']}\nhead: {info['headRefOid']}\n"
            f"published_at: {now()}\nsessions: {json.dumps(all_sessions)}\n"
            + (f"since: {args.since}\n" if args.since else "")
            + "---\n\n"
        )
        (staging / "rationale.md").write_text(header + body, encoding="utf-8")
        # Redact before truncating: a cut through a secret leaves a fragment
        # too short for gitleaks to recognize.
        replaced = redact(staging)

        packed = staging / "packed"
        for name in names:
            path = staging / "sessions" / name
            truncate_session(path, args.max_string)
            dest = packed / f"{name}.gz"
            dest.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "rb") as src, gzip.open(dest, "wb") as out:
                shutil.copyfileobj(src, out)
            sizes[name] = f"{sizes[name] / 1e6:.1f} MB raw -> {dest.stat().st_size / 1e6:.2f} MB"
        total = sum(p.stat().st_size for p in packed.rglob("*.gz"))
        if total > args.max_mb * 1e6:
            raise Abort(
                f"sessions are {total / 1e6:.1f} MB after compaction, over the {args.max_mb} MB limit "
                f"({json.dumps(sizes)}). Pass --since <time this change started> to drop earlier work, "
                "or leave out a session."
            )

        with PrivateCheckout(pr_dir) as repo:
            rels = ["rationale.md"]
            target = repo.path("rationale.md")
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(staging / "rationale.md", target)
            for name in names:
                dest = repo.path(f"sessions/{name}.gz")
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(packed / f"{name}.gz", dest)
                rels.append(f"sessions/{name}.gz")
            commit = repo.commit_and_push(
                f"greptimedb pr {info['number']}: review context at {info['headRefOid'][:7]}", rels
            )
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    print(json.dumps({
        "status": "published" if commit else "unchanged",
        "path": f"{PRIVATE}/{pr_dir}",
        "commit": commit,
        "sessions": sizes,
        "redacted": replaced,
        "not_scanned_for": "internal hostnames/IPs, customer names, personal data",
    }, indent=2))


# -------------------------------------------------------------- reviewer side


def load_state(workdir):
    path = Path(workdir) / "state.json"
    if not path.is_file():
        raise Abort(f"{workdir} is not a prepared review workdir; run `prepare` first")
    return json.loads(path.read_text(encoding="utf-8"))


def save_state(workdir, state):
    (Path(workdir) / "state.json").write_text(json.dumps(state, indent=2), encoding="utf-8")


def review_rel(state):
    return f"reviews/{state['login']}-{state['head'][:7]}.md"


def remove_workdir(workdir):
    workdir = Path(workdir)
    if (workdir / "src").exists():
        run(["git", "worktree", "remove", "--force", str(workdir / "src")], cwd=REPO_ROOT, check=False)
    shutil.rmtree(workdir, ignore_errors=True)


def cmd_prepare(args):
    info = pr_info(args.pr)
    head = info["headRefOid"]
    root = Path(tempfile.gettempdir()) / "greptimedb-review" / str(info["number"])
    # Earlier rounds may have downloaded the rationale; a blind reviewer with
    # plain file access must not find it.
    if root.exists():
        for old in root.iterdir():
            remove_workdir(old)
    workdir = root / head[:7]
    workdir.mkdir(parents=True)

    body = info["body"] or ""
    (workdir / "pr.md").write_text(f"# {info['title']}\n\n{body}\n", encoding="utf-8")
    diff = run(["gh", "pr", "diff", str(info["number"]), "--repo", UPSTREAM]).stdout
    (workdir / "diff.patch").write_text(diff, encoding="utf-8")
    run(["git", "fetch", "--quiet", "--no-tags", f"https://github.com/{UPSTREAM}.git",
         f"pull/{info['number']}/head"], cwd=REPO_ROOT)
    fetched = run(["git", "rev-parse", "FETCH_HEAD"], cwd=REPO_ROOT).stdout.strip()
    if fetched != head:
        # The diff above may belong to either head; start over rather than mix them.
        shutil.rmtree(workdir, ignore_errors=True)
        raise Abort(f"PR head moved from {head[:7]} to {fetched[:7]} during prepare; run prepare again")
    run(["git", "worktree", "add", "--quiet", "--detach", str(workdir / "src"), head], cwd=REPO_ROOT)
    shutil.copyfile(REVIEW_CRITERIA, workdir / "criteria.md")
    # Personal review preferences live in the reviewer's own checkout, which
    # is not necessarily where this script is installed.
    toplevel = run(["git", "rev-parse", "--show-toplevel"]).stdout.strip()
    preferences = Path(toplevel) / ".local" / "review.md"
    if preferences.is_file():
        shutil.copyfile(preferences, workdir / "preferences.md")

    mode = "full" if has_private_access() else "blind-only"
    state = {
        "pr": info["number"],
        "head": head,
        "mode": mode,
        "login": gh_login(),
        "prefix": args.prefix,
    }
    save_state(workdir, state)
    print(json.dumps({
        "workdir": str(workdir),
        "head": head,
        "mode": mode,
        "preferences": str(preferences) if preferences.is_file() else None,
    }, indent=2))


def credential_free_env(workdir, agent):
    """Environment with no GitHub credentials, but with network access.

    The blind reviewer may read public code (e.g. Prometheus) but must not
    reach the private repo. Every credential source gh and git consult is cut
    here; `assert_private_unreachable` verifies the result instead of trusting
    this list. This keeps a well-behaved agent from reading ahead; it is not a
    filesystem sandbox.
    """
    home = Path.home()
    fake_home = Path(workdir) / ".blind-home"
    (fake_home / "gh").mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    for var in ("GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN", "GITHUB_ENTERPRISE_TOKEN",
                "SSH_AUTH_SOCK", "GIT_ASKPASS", "SSH_ASKPASS"):
        env.pop(var, None)
    env.update({
        "HOME": str(fake_home),
        "USERPROFILE": str(fake_home),
        "GH_CONFIG_DIR": str(fake_home / "gh"),
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "credential.helper",
        "GIT_CONFIG_VALUE_0": "",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_ALLOW_PROTOCOL": "https:file",
        "CARGO_HOME": os.environ.get("CARGO_HOME", str(home / ".cargo")),
        "RUSTUP_HOME": os.environ.get("RUSTUP_HOME", str(home / ".rustup")),
    })
    if agent == "codex":
        env["CODEX_HOME"] = os.environ.get("CODEX_HOME", str(home / ".codex"))
    if agent == "opencode":
        # Login lives in the data dir. The config dir stays in the empty fake
        # home so user-configured MCP servers are not loaded.
        env["XDG_DATA_HOME"] = os.environ.get("XDG_DATA_HOME", str(home / ".local" / "share"))
    return env


def assert_private_unreachable(env, cwd):
    probes = {
        "gh api": ["gh", "api", f"repos/{PRIVATE}"],
        "git https": ["git", "ls-remote", f"https://github.com/{PRIVATE}.git"],
        "git ssh": ["git", "ls-remote", f"git@github.com:{PRIVATE}.git"],
    }
    for name, cmd in probes.items():
        try:
            proc = subprocess.run(cmd, cwd=cwd, env=env, capture_output=True, timeout=60)
        except FileNotFoundError:
            continue
        except subprocess.TimeoutExpired:
            raise Abort(f"isolation check `{name}` timed out; cannot confirm the private repo is unreachable")
        if proc.returncode == 0:
            raise Abort(f"isolation check failed: `{name}` can still reach {PRIVATE}; blind review not started")


def blind_prompt(workdir, language):
    text = BLIND_REVIEWER.read_text(encoding="utf-8")
    if text.startswith("---"):
        text = text.split("---", 2)[2]
    return f"{text.strip()}\n\nWorkdir: {workdir}\nLanguage: {language}\n"


def cmd_blind_run(args):
    workdir = Path(args.workdir).resolve()
    load_state(workdir)
    env = credential_free_env(workdir, args.agent)
    assert_private_unreachable(env, workdir)
    out = workdir / "blind.md"
    prompt = blind_prompt(workdir, args.language)
    if args.agent == "codex":
        run([
            # The user config and ChatGPT app connectors add MCP tools (session
            # search, GitHub) that can reach the private repo or local sessions.
            "codex", "exec", "--ignore-user-config", "--disable", "apps",
            "--skip-git-repo-check", "-s", "workspace-write",
            "-c", "sandbox_workspace_write.network_access=true",
            "-C", str(workdir), "-o", str(out), prompt,
        ], cwd=workdir, env=env, timeout=3600)
    else:
        proc = run(["opencode", "run", prompt], cwd=workdir, env=env, timeout=3600)
        out.write_text(proc.stdout, encoding="utf-8")
    print(str(out))


def check_blind(text):
    for heading in BLIND_SECTIONS:
        match = re.search(rf"^{re.escape(heading)}\s*$(.*?)(?=^## |\Z)", text, re.M | re.S)
        if not match or not match.group(1).strip():
            raise Abort(f"blind review must have a non-empty `{heading}` section")


def cmd_record_blind(args):
    workdir = Path(args.workdir)
    state = load_state(workdir)
    if state.get("opened_at"):
        raise Abort("the rationale was already opened in this workdir; a blind review can no longer be recorded")
    if not (workdir / "blind.md").is_file():
        raise Abort(f"write the blind review to {workdir / 'blind.md'} first")
    blind = (workdir / "blind.md").read_text(encoding="utf-8")
    check_blind(blind)
    content = (
        f"---\npr: {state['pr']}\nhead: {state['head']}\nreviewer: {state['login']}\n"
        f"blind_isolation: {args.isolation}\nblind_recorded_at: {now()}\n---\n\n"
        f"# Phase 1: blind review\n\n{blind.strip()}\n"
    )
    if args.isolation == "none":
        content = content.replace("# Phase 1: blind review", "# Phase 1: blind review (blind phase not isolated)")
    if state["mode"] != "full":
        (workdir / "review.md").write_text(content, encoding="utf-8")
        print(json.dumps({"status": "recorded-locally", "file": str(workdir / "review.md")}))
        return
    rel = review_rel(state)
    pr_dir = f"{state['prefix']}/{state['pr']}"
    if private_raw(f"{pr_dir}/{rel}") is not None:
        raise Abort(f"a blind review for this head is already recorded at {pr_dir}/{rel}; continue with `open`")
    with PrivateCheckout(pr_dir) as repo:
        target = repo.path(rel)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        commit = repo.commit_and_push(f"greptimedb pr {state['pr']}: blind review by {state['login']}", [rel])
    print(json.dumps({"status": "recorded", "path": f"{PRIVATE}/{pr_dir}/{rel}", "commit": commit}))


def cmd_open(args):
    workdir = Path(args.workdir)
    state = load_state(workdir)
    if state["mode"] != "full":
        raise Abort(f"no access to {PRIVATE}; this review stops after the blind phase")
    pr_dir = f"{state['prefix']}/{state['pr']}"
    # The review recorded in the private repo is the gate, not local state, so
    # re-running `prepare` for the same head can resume here.
    review = private_raw(f"{pr_dir}/{review_rel(state)}")
    if review is None:
        raise Abort("record the blind review for the current head first (`record-blind`)")
    if not (workdir / "blind.md").exists():
        (workdir / "blind.md").write_bytes(review)
    private = workdir / "private"
    private.mkdir(exist_ok=True)
    rationale = private_raw(f"{pr_dir}/rationale.md")
    files = [p[len(pr_dir) + 1:] for p in private_list(pr_dir)]
    rationale_head = None
    if rationale is not None:
        (private / "rationale.md").write_bytes(rationale)
        match = re.search(rb"^head: (\w+)$", rationale, re.M)
        rationale_head = match.group(1).decode() if match else None
    state["opened_at"] = now()
    save_state(workdir, state)
    print(json.dumps({
        "rationale": str(private / "rationale.md") if rationale is not None else None,
        "rationale_head": rationale_head,
        "reviewed_head": state["head"],
        "files": [f for f in files if f != "rationale.md"],
    }, indent=2))


def cmd_fetch(args):
    workdir = Path(args.workdir)
    state = load_state(workdir)
    if not state.get("opened_at"):
        raise Abort("open the private context first (`open`)")
    data = private_raw(f"{state['prefix']}/{state['pr']}/{args.name}")
    if data is None:
        raise Abort(f"{args.name} not found")
    dest = workdir / "private" / args.name.removesuffix(".gz")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(gzip.decompress(data) if args.name.endswith(".gz") else data)
    print(str(dest))


def cmd_record_compare(args):
    workdir = Path(args.workdir)
    state = load_state(workdir)
    if not state.get("opened_at"):
        raise Abort("open the private context first (`open`)")
    compare = (workdir / "compare.md").read_text(encoding="utf-8").strip()
    if not compare:
        raise Abort("compare.md is empty")
    rel = review_rel(state)
    pr_dir = f"{state['prefix']}/{state['pr']}"
    with PrivateCheckout(pr_dir) as repo:
        target = repo.path(rel)
        target.write_text(
            target.read_text(encoding="utf-8").rstrip()
            + f"\n\n# Phase 2: comparison with rationale\n\n"
            f"Rationale opened at {state['opened_at']}.\n\n"
            + compare + "\n",
            encoding="utf-8",
        )
        commit = repo.commit_and_push(f"greptimedb pr {state['pr']}: comparison by {state['login']}", [rel])
    print(json.dumps({"status": "recorded", "commit": commit}))


def cmd_cleanup(args):
    remove_workdir(args.workdir)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--prefix", default="greptimedb",
                        help="top-level directory in the private repo (change only for testing)")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("access", help="print `full` or `none`").set_defaults(func=cmd_access)

    p = sub.add_parser("publish", help="redact and push rationale.md and sessions for a PR")
    p.add_argument("--pr", type=int, required=True)
    what = p.add_mutually_exclusive_group(required=True)
    what.add_argument("--rationale", help="full rationale; replaces any published one")
    what.add_argument("--revision", help="what changed in this round; appended to the published rationale")
    p.add_argument("--agent", choices=("claude", "codex", "opencode"), required=True,
                   help="the agent running this command; its current session is included")
    p.add_argument("--session", action="append", default=[], metavar="AGENT:ID",
                   help="extra session to include, e.g. codex:019a...")
    p.add_argument("--no-current-session", action="store_true")
    p.add_argument("--since", metavar="ISO8601",
                   help="drop session records before this time, e.g. 2026-10-12T05:40:00Z")
    p.add_argument("--max-string", type=int, default=4000,
                   help="truncate any string longer than this many characters (default 4000)")
    p.add_argument("--max-mb", type=float, default=20,
                   help="refuse to publish when compressed sessions exceed this size (default 20)")
    p.set_defaults(func=cmd_publish)

    p = sub.add_parser("prepare", help="create a review workdir for a PR")
    p.add_argument("--pr", type=int, required=True)
    p.set_defaults(func=cmd_prepare)

    p = sub.add_parser("blind-run", help="run the blind reviewer in a credential-free subprocess")
    p.add_argument("--workdir", required=True)
    p.add_argument("--agent", choices=("codex", "opencode"), required=True)
    p.add_argument("--language", default="English", help="language of the blind review, e.g. Chinese")
    p.set_defaults(func=cmd_blind_run)

    p = sub.add_parser("record-blind", help="record <workdir>/blind.md before anything private is read")
    p.add_argument("--workdir", required=True)
    p.add_argument("--isolation", choices=ISOLATION_MODES, required=True)
    p.set_defaults(func=cmd_record_blind)

    p = sub.add_parser("open", help="download the rationale after the blind review is recorded")
    p.add_argument("--workdir", required=True)
    p.set_defaults(func=cmd_open)

    p = sub.add_parser("fetch", help="download one file listed by `open` (sessions are decompressed)")
    p.add_argument("--workdir", required=True)
    p.add_argument("--name", required=True, help="path relative to the PR directory, as listed by `open`")
    p.set_defaults(func=cmd_fetch)

    p = sub.add_parser("record-compare", help="append <workdir>/compare.md to the review file")
    p.add_argument("--workdir", required=True)
    p.set_defaults(func=cmd_record_compare)

    p = sub.add_parser("cleanup", help="remove a review workdir and its worktree")
    p.add_argument("--workdir", required=True)
    p.set_defaults(func=cmd_cleanup)

    args = parser.parse_args()
    try:
        args.func(args)
    except Abort as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
