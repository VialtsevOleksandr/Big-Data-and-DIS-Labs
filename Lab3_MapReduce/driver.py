import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4


ROOT = Path(__file__).resolve().parent
SCRIPTS = ("amounts.py", "mapper1.py", "reducer1.py", "mapper2.py", "reducer2.py")


def command(args):
    result = subprocess.run(args, text=True, encoding="utf-8", errors="replace",
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {args!r}\n"
            f"{result.stdout}\n{result.stderr}"
        )
    return result.stdout.strip()


def docker_exec(container, *args):
    return command(["docker", "exec", container, *args])


def run_job(container, args, log_path):
    # Hadoop progress and counters go to stderr; save both streams to the log.
    with log_path.open("w", encoding="utf-8") as log:
        result = subprocess.run(["docker", "exec", container, *args],
                                stdout=log, stderr=subprocess.STDOUT)
    if result.returncode:
        tail = "\n".join(log_path.read_text(encoding="utf-8", errors="replace")
                         .splitlines()[-50:])
        raise RuntimeError(f"Hadoop job failed. Log: {log_path}\n{tail}")


def streaming_args(remote_dir, input_path, output_path, job_number, reducers,
                   use_combiner):
    filenames = (f"mapper{job_number}.py", f"reducer{job_number}.py", "amounts.py")
    args = [
        "mapred", "streaming",
        "-D", f"mapreduce.job.name=lab3-job{job_number}",
        "-D", f"mapreduce.job.reduces={reducers}",
        "-files", ",".join(f"{remote_dir}/{name}" for name in filenames),
        "-input", input_path,
        "-output", output_path,
        "-mapper", f"python3 mapper{job_number}.py",
        "-reducer", f"python3 reducer{job_number}.py",
        # -files creates symlinks; explicitly include the task working directory.
        "-cmdenv", "PYTHONPATH=.",
    ]
    if job_number == 1 and use_combiner:
        args += ["-combiner", "python3 reducer1.py"]
    return args


def run_job1(container, remote_dir, input_path, output_path, reducers,
             use_combiner, log_path):
    run_job(container, streaming_args(remote_dir, input_path, output_path, 1,
                                     reducers, use_combiner), log_path)


def run_job2(container, remote_dir, daily_path, output_path, log_path):
    # Обов'язково один reducer, бо він порівнює підсумки ВСІХ категорій.
    run_job(container, streaming_args(remote_dir, daily_path, output_path, 2,
                                     1, False), log_path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="?", type=Path, default=ROOT / "transactions.csv")
    parser.add_argument("--container", default="hadoop-namenode-1")
    parser.add_argument("--worker-container", default="hadoop-nodemanager-1")
    parser.add_argument("--reducers1", type=int, default=2)
    parser.add_argument("--no-combiner", action="store_true")
    args = parser.parse_args()
    if args.reducers1 < 1:
        parser.error("--reducers1 must be at least 1")
    input_file = args.input.resolve()
    if not input_file.is_file():
        parser.error(f"Input file not found: {input_file}")
    for name in SCRIPTS:
        if not (ROOT / name).is_file():
            parser.error(f"Missing script: {ROOT / name}")

    for container in {args.container, args.worker_container}:
        docker_exec(container, "python3", "--version")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid4().hex[:8]
    local_dir = ROOT / "runs" / run_id
    local_dir.mkdir(parents=True)
    remote_dir = f"/tmp/lab3-mapreduce/{run_id}"
    hdfs_dir = f"/lab3-mapreduce/{run_id}"
    input_path = f"{hdfs_dir}/input/transactions.csv"
    daily_path = f"{hdfs_dir}/daily"
    winner_path = f"{hdfs_dir}/winner"

    metadata = {
        "run_id": run_id,
        "input_file": str(input_file),
        "input_bytes": input_file.stat().st_size,
        "input_sha256": hashlib.sha256(input_file.read_bytes()).hexdigest(),
        "container": args.container,
        "worker_container": args.worker_container,
        "job1_reducers": args.reducers1,
        "job2_reducers": 1,
        "combiner_job1": not args.no_combiner,
        "hdfs_input": input_path,
        "hdfs_job1_output": daily_path,
        "hdfs_job2_input": daily_path,
        "hdfs_job2_output": winner_path,
        "status": "running",
    }
    metadata_path = local_dir / "run.json"

    def save_metadata():
        metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2)
                                 + "\n", encoding="utf-8")

    save_metadata()
    print(f"Run directory: {local_dir}", flush=True)
    try:
        docker_exec(args.container, "mkdir", "-p", remote_dir)
        for name in SCRIPTS:
            command(["docker", "cp", str(ROOT / name), f"{args.container}:{remote_dir}/{name}"])
        command(["docker", "cp", str(input_file), f"{args.container}:{remote_dir}/transactions.csv"])
        docker_exec(args.container, "hdfs", "dfs", "-mkdir", "-p", f"{hdfs_dir}/input")
        docker_exec(args.container, "hdfs", "dfs", "-put",
                    f"{remote_dir}/transactions.csv", input_path)
        metadata["hdfs_block_bytes"] = int(docker_exec(
            args.container, "hdfs", "dfs", "-stat", "%o", input_path))
        save_metadata()

        print("Job 1: daily totals (progress: runs/.../job1.log)", flush=True)
        run_job1(args.container, remote_dir, input_path, daily_path, args.reducers1,
                 not args.no_combiner, local_dir / "job1.log")
        print("Job 2: category winner (progress: runs/.../job2.log)", flush=True)
        run_job2(args.container, remote_dir, daily_path, winner_path, local_dir / "job2.log")

        for hdfs_path, filename in ((daily_path, "daily_totals.tsv"),
                                     (winner_path, "winner.tsv")):
            text = docker_exec(args.container, "hdfs", "dfs", "-cat", f"{hdfs_path}/part-*")
            (local_dir / filename).write_text(text + ("\n" if text else ""), encoding="utf-8")
        metadata["status"] = "success"
        save_metadata()
        print("Winner: " + (local_dir / "winner.tsv").read_text(encoding="utf-8").strip())
        print(f"Logs, daily totals and run settings: {local_dir}")
    except Exception as exc:
        metadata["status"] = "failed"
        metadata["error"] = str(exc)
        save_metadata()
        raise


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
