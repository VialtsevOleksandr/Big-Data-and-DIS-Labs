import dataclasses
import json
import pytest

from salesgen.config import Config
from salesgen.stats import compute
from salesgen.writer import run


def test_invalid_overwrite_preserves_existing_data(tiny, tiny_cfg):
    manifest = json.loads((tiny / "manifest.json").read_text(encoding="utf-8"))
    path = tiny / manifest["data"]["files"][0]["path"]
    previous = path.read_bytes()
    with pytest.raises(ValueError):
        run(dataclasses.replace(tiny_cfg, n_receipts=0), tiny, overwrite=True, log=lambda *a: None)
    assert path.read_bytes() == previous


def test_overwrite_keeps_unrelated_csv(tmp_path, tiny_cfg):
    out = tmp_path / "owned"
    cfg = dataclasses.replace(tiny_cfg, n_receipts=100, start_date="2025-01-01", end_date="2025-01-03")
    first = run(cfg, out, log=lambda *a: None)
    unrelated = out / "data" / "keep.csv"
    unrelated.write_text("my,data\n", encoding="utf-8")
    second = run(cfg, out, overwrite=True, log=lambda *a: None)
    assert unrelated.read_text(encoding="utf-8") == "my,data\n"
    assert first["data"]["data_fingerprint_sha256"] == second["data"]["data_fingerprint_sha256"]


def test_overwrite_rejects_unmanaged_output(tmp_path):
    out = tmp_path / "unmanaged"
    out.mkdir()
    sentinel = out / "important.csv"
    sentinel.write_text("keep\n", encoding="utf-8")
    cfg = Config(n_receipts=100, n_products=300, n_stores=20, end_date="2025-01-03")
    with pytest.raises(ValueError, match="manifest.json"):
        run(cfg, out, overwrite=True, log=lambda *a: None)
    assert sentinel.read_text(encoding="utf-8") == "keep\n"


def test_sampling_size_and_repeatability(tiny):
    first = compute(tiny, itemset_sample=100)["itemsets"]
    second = compute(tiny, itemset_sample=100)["itemsets"]
    assert first == second
    assert first["sample_receipts"] == 100
    assert first["sampling"] == "uniform_without_replacement"
    with pytest.raises(ValueError):
        compute(tiny, itemset_sample=0)
