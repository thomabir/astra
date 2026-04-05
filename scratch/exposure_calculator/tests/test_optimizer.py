"""Tests for ExposureOptimizer — fully offline.

The key trick: we inject stub ImageSources that return pre-built arrays,
so the optimizer's iteration logic can be tested without any camera.
"""

import numpy as np
import pytest

from exposure_calculator.interfaces import ImageSource
from exposure_calculator.models import CameraParams
from exposure_calculator.optimizer import ExposureOptimizer, _suggest_next_exptime


# ---------------------------------------------------------------------------
# Stub ImageSources for testing
# ---------------------------------------------------------------------------

class AlwaysGoodSource(ImageSource):
    """Returns a well-exposed sky-dominated image regardless of exptime."""

    def __init__(self, camera: CameraParams):
        self.camera = camera
        self.call_count = 0

    def capture(self, exptime: float) -> np.ndarray:
        self.call_count += 1
        rng = np.random.default_rng(42)
        sky_adu = 800.0
        return rng.poisson(sky_adu, (256, 256)).astype(np.float32)


class AlwaysSaturatingSource(ImageSource):
    """Returns a saturated image regardless of exptime."""

    def __init__(self, camera: CameraParams):
        self.camera = camera

    def capture(self, exptime: float) -> np.ndarray:
        return np.full((256, 256), self.camera.saturation_adu, dtype=np.float32)


class ScalingSource(ImageSource):
    """Sky signal scales linearly with exptime; saturates above a threshold."""

    def __init__(self, camera: CameraParams, adu_per_second: float = 100.0):
        self.camera = camera
        self.adu_per_second = adu_per_second

    def capture(self, exptime: float) -> np.ndarray:
        rng = np.random.default_rng(0)
        sky_adu = min(self.adu_per_second * exptime, self.camera.saturation_adu - 1)
        return rng.poisson(sky_adu, (256, 256)).astype(np.float32)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_good_image_on_first_try_returns_immediately(camera):
    source = AlwaysGoodSource(camera)
    optimizer = ExposureOptimizer(source, camera, initial_exptime=60.0)
    result = optimizer.optimize()
    assert result == pytest.approx(60.0)
    assert source.call_count == 1  # did not iterate


def test_saturated_initial_exptime_results_in_shorter_exptime(camera):
    source = AlwaysSaturatingSource(camera)
    optimizer = ExposureOptimizer(source, camera, initial_exptime=300.0, max_iterations=5)
    result = optimizer.optimize()
    assert result < 300.0


def test_optimizer_respects_max_iterations(camera):
    source = AlwaysSaturatingSource(camera)
    optimizer = ExposureOptimizer(source, camera, initial_exptime=300.0, max_iterations=3)
    # Should return without raising even if never converging
    result = optimizer.optimize()
    assert result > 0


def test_scaling_source_converges(camera):
    # At 1 ADU/s, exptime=600 should saturate; optimizer should find something shorter
    source = ScalingSource(camera, adu_per_second=1000.0)
    optimizer = ExposureOptimizer(source, camera, initial_exptime=600.0, max_iterations=8)
    result = optimizer.optimize()
    assert result < 600.0
    assert result > 0


# ---------------------------------------------------------------------------
# _suggest_next_exptime — pure logic, tested without any images
# ---------------------------------------------------------------------------

def test_suggest_shorter_exptime_when_saturated(camera):
    from exposure_calculator.models import ExposureAssessment
    saturated = ExposureAssessment(
        saturated=True,
        noise_dominated=False,
        sky_background_adu=60_000.0,
        noise_ratio=0.1,
        saturation_fraction=0.5,
    )
    next_exptime = _suggest_next_exptime(current_exptime=300.0, assessment=saturated)
    assert next_exptime < 300.0


def test_suggest_longer_exptime_when_noise_dominated(camera):
    from exposure_calculator.models import ExposureAssessment
    noise_dom = ExposureAssessment(
        saturated=False,
        noise_dominated=True,
        sky_background_adu=10.0,
        noise_ratio=3.0,
        saturation_fraction=0.0,
    )
    next_exptime = _suggest_next_exptime(current_exptime=5.0, assessment=noise_dom)
    assert next_exptime > 5.0
