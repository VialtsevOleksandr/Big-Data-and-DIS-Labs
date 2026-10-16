import json
from pathlib import Path
from unittest.mock import patch

from benchmark import save


def test_save_retries_temporary_permission_error(tmp_path):
    replace = Path.replace
    failures = 2

    def transient(source, target):
        nonlocal failures
        if failures:
            failures -= 1
            raise PermissionError("file temporarily locked")
        return replace(source, target)

    report = {"measurements": []}
    with patch.object(Path, "replace", transient), patch("benchmark.time.sleep") as sleep:
        paths = save(report, tmp_path)
    assert sleep.call_count == 2
    assert paths["json"] == tmp_path / "benchmark.json"
    assert json.loads(paths["json"].read_text(encoding="utf-8")) == report
    assert not list(tmp_path.glob("*.tmp"))


def test_locked_destination_preserves_old_file_and_keeps_using_recovery(tmp_path):
    original = tmp_path / "benchmark.json"
    original.write_text('{"old": true}', encoding="utf-8")
    replace = Path.replace

    def locked(source, target):
        if target == original:
            raise PermissionError("destination open without delete sharing")
        return replace(source, target)

    report = {"measurements": []}
    paths = {}
    with patch.object(Path, "replace", locked), patch("benchmark.time.sleep") as sleep:
        save(report, tmp_path, paths)
        recovery = paths["json"]
        report["finished"] = True
        save(report, tmp_path, paths)
    assert sleep.call_count == 5
    assert json.loads(original.read_text(encoding="utf-8")) == {"old": True}
    assert recovery != original and paths["json"] == recovery
    assert json.loads(recovery.read_text(encoding="utf-8")) == report
    assert len(list(tmp_path.glob("benchmark-recovery-*.json"))) == 1


def test_locked_rename_keeps_complete_temporary_snapshot(tmp_path):
    report = {"measurements": []}
    with patch.object(Path, "replace", side_effect=PermissionError("locked")), \
            patch.object(Path, "rename", side_effect=PermissionError("locked")), \
            patch("benchmark.time.sleep"):
        paths = save(report, tmp_path)
    assert paths["json"].suffix == ".tmp"
    assert json.loads(paths["json"].read_text(encoding="utf-8")) == report
    assert "median_ms" in paths["csv"].read_text(encoding="utf-8-sig")
