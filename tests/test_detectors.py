import random

from app.detectors.adwin import ADWINDetector
from app.detectors.page_hinkley import PageHinkleyDetector
from app.detectors.psi import PSIDetector


def test_psi_stable_distribution_reports_stable():
    random.seed(10)
    ref = [random.gauss(0, 1) for _ in range(1000)]
    cur = [random.gauss(0, 1) for _ in range(300)]
    d = PSIDetector(num_bins=10)
    d.fit_reference(ref)
    result = d.score(cur)
    assert result.verdict == "stable"
    assert result.psi < 0.10


def test_psi_shifted_distribution_reports_significant():
    random.seed(11)
    ref = [random.gauss(0, 1) for _ in range(1000)]
    cur = [random.gauss(2, 1) for _ in range(300)]
    d = PSIDetector(num_bins=10)
    d.fit_reference(ref)
    result = d.score(cur)
    assert result.verdict == "significant"
    assert result.psi >= 0.25


def test_psi_requires_fit_before_score():
    d = PSIDetector()
    try:
        d.score([1.0, 2.0])
        assert False, "expected RuntimeError"
    except RuntimeError:
        pass


def test_adwin_detects_mean_shift():
    random.seed(0)
    d = ADWINDetector(delta=0.002)
    drift_index = None
    for i in range(400):
        v = random.gauss(0, 1) if i < 200 else random.gauss(5, 1)
        r = d.update(v)
        if r.drift_detected and drift_index is None:
            drift_index = i
    assert drift_index is not None
    assert drift_index >= 200  # must not fire before the actual shift


def test_adwin_no_drift_on_stable_stream():
    random.seed(5)
    d = ADWINDetector(delta=0.002)
    any_drift = False
    for _ in range(300):
        r = d.update(random.gauss(0, 1))
        any_drift = any_drift or r.drift_detected
    assert not any_drift


def test_page_hinkley_detects_step_change():
    random.seed(1)
    d = PageHinkleyDetector(min_instances=30, threshold=20, mode="up")
    drift_index = None
    for i in range(300):
        v = random.gauss(0, 1) if i < 150 else random.gauss(3, 1)
        r = d.update(v)
        if r.drift_detected and drift_index is None:
            drift_index = i
    assert drift_index is not None
    assert drift_index >= 150


def test_page_hinkley_reset_clears_counters():
    d = PageHinkleyDetector(min_instances=5, threshold=5)
    for i in range(20):
        d.update(float(i))
    assert d.n_updates == 20
    d.reset()
    assert d.n_updates == 0
    assert d.n_drifts == 0
