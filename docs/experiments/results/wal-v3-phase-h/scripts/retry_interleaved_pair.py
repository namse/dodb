import json
import pathlib
import shlex
import subprocess
import time

artifact_root = pathlib.Path(__file__).resolve().parents[1]
local_raw = artifact_root / "raw" / "interleaved" / "durable" / "real"
remote_host = "opc@217.142.246.204"
identity = "/Users/namse/Downloads/ssh-key-2026-09-23.key"
remote_results = "/tmp/dodb-phase-h-interleaved/durable/real"
remote_tmp_root = "/bench/zfs/db/phase-h-tmp"
scenarios = [("h1", "/tmp/dodb-phase-h1/phase0-bench"), ("g0", "/tmp/dodb-phase-h-h0/phase0-bench")]
seed = 982_005_047
for variant, binary in scenarios:
    stem = f"{variant}-real-durable-05-rep2-w64-width16-different-leaf-heavy"
    output = f"{remote_results}/{stem}.jsonl"
    temporary = f"{remote_tmp_root}/{stem}"
    command = [binary, "--engine", "planned-blink", "--suite", "write", "--writers", "64", "--widths", "16", "--distributions", "different-leaf-heavy", "--duration", "5s", "--warmup", "2s", "--repetitions", "1", "--cache-capacity", "256", "--working-set", "100000", "--key-size", "16", "--value-size", "64", "--group-limit", "64", "--group-bytes", "4194304", "--queue-capacity", "256", "--collection-delay", "0us", "--transaction-mode", "unconditional", "--tokio-workers", "2", "--sync-mode", "real", "--parallel-workers", "2", "--parallel-min-mutations", "32", "--seed", str(seed), "--output", output]
    remote_script = f"rm -rf {shlex.quote(temporary)}; mkdir -p {shlex.quote(temporary)}; TMPDIR={shlex.quote(temporary)} perf stat -x, -e cycles,instructions -- {shlex.join(command)}; run_status=$?; rm -rf {shlex.quote(temporary)}; exit $run_status"
    outcome = subprocess.run(["ssh", "-o", "IdentitiesOnly=yes", "-i", identity, remote_host, remote_script], capture_output=True, text=True)
    (local_raw / f"{stem}.log").write_text(outcome.stdout + outcome.stderr, encoding="utf-8")
    if outcome.returncode != 0:
        raise SystemExit(f"{stem} failed; see local log")
    subprocess.run(["scp", "-q", "-o", "IdentitiesOnly=yes", "-i", identity, f"{remote_host}:{output}", str(local_raw / f"{stem}.jsonl")], check=True)
    event = {"variant": variant, "sync_mode": "real", "scenario_set": "durable", "scenario_index": 5, "repetition": 2, "writers": 64, "width": 16, "distribution": "different-leaf-heavy", "seed": seed, "binary": binary, "command": command, "retry_after_ssh_disconnect": True, "started_unix": time.time()}
    with (local_raw / "run-order-retries.jsonl").open("a", encoding="utf-8") as run_file:
        run_file.write(json.dumps(event, sort_keys=True) + "\n")
