import json
import pathlib
import shlex
import subprocess
import sys
import time

artifact_root = pathlib.Path(__file__).resolve().parents[1]
sync_mode = sys.argv[1]
scenario_set = sys.argv[2]
remote_host = "opc@217.142.246.204"
identity = "/Users/namse/Downloads/ssh-key-2026-09-23.key"
remote_root = "/tmp/dodb-phase-h-interleaved"
remote_tmp_root = "/bench/zfs/db/phase-h-tmp"
binaries = {"g0": "/tmp/dodb-phase-h-h0/phase0-bench", "h1": "/tmp/dodb-phase-h1/phase0-bench"}
scenarios = {
    "gate": [(64, 16, "uniform"), (64, 1, "uniform")],
    "durable": [(16, 1, "uniform"), (16, 16, "uniform"), (64, 1, "uniform"), (64, 16, "uniform"), (64, 16, "same-leaf-heavy"), (64, 16, "different-leaf-heavy")],
    "matrix14": [(writer_count, width, distribution) for writer_count in (16, 64) for width in (1, 16) for distribution in ("uniform", "same-leaf-heavy", "different-leaf-heavy")] + [(1, 1, "uniform"), (1, 16, "uniform")],
}[scenario_set]
local_raw = artifact_root / "raw" / "interleaved" / scenario_set / sync_mode
local_raw.mkdir(parents=True, exist_ok=True)
run_order = local_raw / "run-order.jsonl"
remote_results = f"{remote_root}/{scenario_set}/{sync_mode}"
subprocess.run(["ssh", "-o", "IdentitiesOnly=yes", "-i", identity, remote_host, f"mkdir -p {shlex.quote(remote_results)} {shlex.quote(remote_tmp_root)}"], check=True)
with run_order.open("w", encoding="utf-8") as run_order_file:
    for scenario_index, (writer_count, width, distribution) in enumerate(scenarios):
        for repetition in range(1, 4):
            order = ["g0", "h1"] if repetition % 2 else ["h1", "g0"]
            seed = 982_000_000 + scenario_index * 1_009 + repetition
            for variant in order:
                stem = f"{variant}-{sync_mode}-{scenario_set}-{scenario_index:02d}-rep{repetition}-w{writer_count}-width{width}-{distribution}"
                remote_output = f"{remote_results}/{stem}.jsonl"
                remote_temporary = f"{remote_tmp_root}/{stem}"
                command = [binaries[variant], "--engine", "planned-blink", "--suite", "write", "--writers", str(writer_count), "--widths", str(width), "--distributions", distribution, "--duration", "5s", "--warmup", "2s", "--repetitions", "1", "--cache-capacity", "256", "--working-set", "100000", "--key-size", "16", "--value-size", "64", "--group-limit", "64", "--group-bytes", "4194304", "--queue-capacity", "256", "--collection-delay", "0us", "--transaction-mode", "unconditional", "--tokio-workers", "2", "--sync-mode", sync_mode, "--parallel-workers", "2", "--parallel-min-mutations", "32", "--seed", str(seed), "--output", remote_output]
                remote_script = f"mkdir -p {shlex.quote(remote_temporary)} && TMPDIR={shlex.quote(remote_temporary)} perf stat -x, -e cycles,instructions -- {shlex.join(command)} && rm -rf {shlex.quote(remote_temporary)}"
                event = {"variant": variant, "sync_mode": sync_mode, "scenario_set": scenario_set, "scenario_index": scenario_index, "repetition": repetition, "writers": writer_count, "width": width, "distribution": distribution, "seed": seed, "binary": binaries[variant], "command": command, "started_unix": time.time()}
                run_order_file.write(json.dumps(event, sort_keys=True) + "\n")
                run_order_file.flush()
                outcome = subprocess.run(["ssh", "-o", "IdentitiesOnly=yes", "-i", identity, remote_host, remote_script], capture_output=True, text=True)
                (local_raw / f"{stem}.log").write_text(outcome.stdout + outcome.stderr, encoding="utf-8")
                if outcome.returncode != 0:
                    raise SystemExit(f"{stem} failed; see local log")
                subprocess.run(["scp", "-q", "-o", "IdentitiesOnly=yes", "-i", identity, f"{remote_host}:{remote_output}", str(local_raw / f"{stem}.jsonl")], check=True)
