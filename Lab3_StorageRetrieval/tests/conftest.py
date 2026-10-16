import dataclasses
from pathlib import Path

import pytest

from salesgen.config import Config
from salesgen.writer import run

TINY = Config(seed=20050406, n_receipts=3000, n_stores=20, n_products=300, batch_size=500)


def gen(tmp_path_factory, name: str, cfg: Config) -> Path:
    out = tmp_path_factory.mktemp(name)
    run(cfg, out, overwrite=True, log=lambda *a: None)
    return out


@pytest.fixture(scope="session")
def tiny(tmp_path_factory) -> Path:
    return gen(tmp_path_factory, "tiny", TINY)


@pytest.fixture(scope="session")
def tiny_cfg() -> Config:
    return TINY


@pytest.fixture(scope="session")
def make(tmp_path_factory):
    def _make(name: str, **over) -> Path:
        return gen(tmp_path_factory, name, dataclasses.replace(TINY, **over))
    return _make
