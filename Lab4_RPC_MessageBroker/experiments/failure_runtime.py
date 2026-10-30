"""Docker worker lifecycle and incremental ID ledgers for experiment 4."""
from collections import Counter
from contextlib import contextmanager
import json
from pathlib import Path
import subprocess
from uuid import uuid4

from experiments.common import ROOT, write_json
from experiments.compat_runtime import audit_rows, wait_for


def docker(*arguments, timeout=60):
    result = subprocess.run(["docker", *arguments], cwd=ROOT, capture_output=True,
                            encoding="utf-8", timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"docker {' '.join(arguments[:3])} failed: {result.stderr.strip()}")
    return result.stdout


class DockerWorker:
    def __init__(self, directory, namespace, checkpoint=None, pause_ms=60000, broker_url=None):
        self.directory = Path(directory).resolve()
        self.instance = uuid4().hex
        self.name = "dis-lab4-failure-" + self.instance[:16]
        self.namespace = namespace
        self.audit = self.directory / "warehouse.jsonl"
        self.checkpoint = checkpoint
        self.pause_ms = pause_ms
        self.broker_url = broker_url or "amqp://dis:dis-local@rabbitmq:5672/%2F"
        self.started = False

    def inspect(self):
        value = json.loads(docker("inspect", self.name))[0]
        if value["Config"].get("Labels", {}).get("dis-lab4.failure-instance") != self.instance:
            raise RuntimeError("Worker identity does not match this experiment; refusing to stop/kill it")
        return value

    def require_running(self):
        state = self.inspect()["State"]
        if not state["Running"]:
            raise RuntimeError(f"Experiment worker exited ({state['ExitCode']}); rebuild with docker compose build warehouse and inspect its log")

    def start(self):
        args = ["run", "--detach", "--init", "--name", self.name, "--network", "dis-lab4_default",
                "--label", "dis-lab4.failure-instance=" + self.instance,
                "--mount", f"type=bind,source={self.directory},target=/evidence",
                "dis-lab4-warehouse:local", "--schema", "v1", "--namespace", self.namespace,
                "--broker-url", self.broker_url, "--audit", "/evidence/warehouse.jsonl",
                "--instance-id", self.instance, "--compact-audit", "--audit-buffered", "--quiet"]
        if self.checkpoint:
            args += ["--delay-order-id", self.checkpoint, "--delay-before-ack-ms", str(self.pause_ms)]
        self.container_id = docker(*args).strip()
        self.started = True
        def ready():
            self.require_running()
            return any(row["kind"] == "ready" and row.get("instance_id") == self.instance
                       for row in audit_rows(self.audit))
        wait_for(ready, "Docker warehouse readiness", timeout=45)
        write_json(self.directory / f"worker_{self.instance[:8]}.json", dict(
            name=self.name, instance_id=self.instance, container_id=self.container_id,
            image_id=self.inspect()["Image"], checkpoint_order_id=self.checkpoint))
        return self

    def hard_kill(self):
        self.require_running()
        before = self.inspect()
        docker("kill", "--signal", "KILL", self.name)
        def exited():
            value = self.inspect()
            return value if not value["State"]["Running"] else None
        after = wait_for(exited, "SIGKILL exit", timeout=10)
        record = dict(command=["docker", "kill", "--signal", "KILL", self.name],
                      container_id=self.container_id, instance_id=self.instance,
                      exit_code=after["State"]["ExitCode"], oom_killed=after["State"].get("OOMKilled", False),
                      started_at=before["State"]["StartedAt"], finished_at=after["State"]["FinishedAt"])
        if record["exit_code"] != 137 or record["oom_killed"]:
            raise RuntimeError("The worker did not exit by the intended SIGKILL (expected exit 137, no OOM)")
        write_json(self.directory / "docker_kill.json", record)
        return record

    def close(self):
        if self.started:
            try:
                state = self.inspect()["State"]
                if state["Running"]:
                    docker("stop", "--time", "3", self.name, timeout=15)
                log = subprocess.run(["docker", "logs", self.name], capture_output=True, encoding="utf-8", timeout=15)
                (self.directory / f"worker_{self.instance[:8]}.log").write_text(log.stdout + log.stderr, encoding="utf-8")
            finally:
                # The name/label was checked above; this container belongs to this invocation.
                self.inspect()
                docker("rm", "--force", self.name)


@contextmanager
def docker_worker(*args, **kwargs):
    worker = DockerWorker(*args, **kwargs)
    try:
        yield worker.start()
    finally:
        worker.close()


class AuditTail:
    def __init__(self, path):
        self.path = Path(path)
        self.offset = 0

    def read(self):
        rows = []
        if not self.path.exists():
            return rows
        with self.path.open(encoding="utf-8") as stream:
            stream.seek(self.offset)
            while True:
                position = stream.tell()
                line = stream.readline()
                if not line or not line.endswith("\n"):
                    self.offset = position
                    break
                rows.append(json.loads(line))
                self.offset = stream.tell()
        return rows


class Ledger:
    def __init__(self, directory, server):
        self.worker_tail = AuditTail(Path(directory) / "warehouse.jsonl")
        self.result_tail = AuditTail(server.results)
        self.publish_tail = AuditTail(server.publications)
        self.processed, self.acked, self.reserved = Counter(), Counter(), Counter()
        self.results, self.published, self.redelivered = Counter(), Counter(), Counter()
        self.instances = {}
        self.checkpoints = []
        self.duplicate_results = 0

    def update(self):
        for row in self.worker_tail.read():
            kind = row["kind"]
            if kind == "processed":
                key = row["order_id"]
                self.processed[key] += 1
                self.reserved[key] += int(row["reserved"])
                self.redelivered[key] += int(row["redelivered"])
                self.instances.setdefault(key, set()).add(row["instance_id"])
            elif kind == "acked":
                self.acked[row["order_id"]] += 1
            elif kind == "before_ack":
                self.checkpoints.append(row)
        for row in self.result_tail.read():
            if row["kind"] == "result":
                self.results[row["order_id"]] += 1
                self.duplicate_results += int(not row["updated"])
            elif row["kind"] == "result_rejected":
                raise RuntimeError(f"Order service rejected a warehouse result: {row}")
        for row in self.publish_tail.read():
            if row["kind"] == "published":
                self.published[row["order_id"]] += 1

    def counts(self):
        return dict(processing_attempts=sum(self.processed.values()), unique_processed=len(self.processed),
                    unique_acked=len(self.acked), duplicate_attempts=sum(n - 1 for n in self.processed.values()),
                    result_deliveries=sum(self.results.values()), unique_results=len(self.results),
                    duplicate_results_ignored=self.duplicate_results, confirmed_publications=sum(self.published.values()))
