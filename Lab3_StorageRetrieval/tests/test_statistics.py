"""Статистичні перевірки на наборі 20 000 чеків з повним каталогом (2 000 товарів).

Пороги навмисно грубі (далекі від очікуваних значень), щоб тест не був
крихким, але ловив поломку моделі (відсутність кореляцій, «один товар
забирає все», рівномірні довжини тощо)."""
import dataclasses

import pytest

from salesgen.config import Config
from salesgen.stats import compute


@pytest.fixture(scope="module")
def stats20k(tmp_path_factory):
    from salesgen.writer import run
    out = tmp_path_factory.mktemp("s20k")
    run(dataclasses.replace(Config(), n_receipts=20_000), out, log=lambda *a: None)
    return compute(out, itemset_sample=5_000)


def test_basket_length_shape(stats20k):
    s = stats20k
    assert 9.0 < s["mean_lines"] < 11.0
    assert s["length"]["share_le_3"] > 0.2 and s["length"]["share_ge_20"] > 0.08
    assert s["length"]["max"] <= 30


def test_popularity_skewed_but_not_degenerate(stats20k):
    t, p = stats20k["types"], stats20k["products"]
    assert t["top10"][0][2] < 0.10                     # жоден тип не домінує
    assert p["top1_share"] < 0.03                      # жоден товар не забирає обсяг
    assert p["top10pct_share"] > 0.3                   # але розподіл помітно нерівномірний
    assert p["sold"] == p["total"] == 2000


def test_designed_associations_exceed_baseline(stats20k):
    lf = stats20k["lift"]
    base = lf["baseline_median_size_adjusted_lift_non_mission_pairs"]
    assert 0.7 < base < 1.3
    strong = {("Буряк", "Капуста"), ("Чай", "Печиво"), ("Макарони", "Соус для пасти"),
              ("Корм для котів", "Наповнювач для котів")}
    rows = [r for r in lf["mission_pairs"] if (r["A"], r["B"]) in strong]
    assert len(rows) == len(strong)
    for r in rows:
        assert r["enough_data"]
        assert r["lift_size_adjusted"] > 1.3 * base, r
        assert r["P(B|A)_95ci"][0] > r["P(B)"], r       # нижня межа CI вища за безумовну частоту


def test_time_and_stores(stats20k):
    tm, st = stats20k["time"], stats20k["stores"]
    assert tm["days_with_sales"] == 365
    wd = tm["by_weekday_mon0"]
    assert wd[5] > wd[0]                                # субота > понеділка
    assert tm["by_month"]["2025-12"] > tm["by_month"]["2025-02"]
    h = tm["by_hour_utc"]
    assert h[17] > h[7] > h[2]
    assert st["with_sales"] == 100 and st["max_to_min"] > 2
