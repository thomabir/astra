"""Shared fixtures for exposure calculator tests."""

import numpy as np
import pytest

from exposure_calculator.models import CameraParams

# A representative camera. Values are realistic but not tied to any real instrument.
CAMERA = CameraParams(
    gain=1.5,          # e-/ADU
    ron=5.0,           # e- RMS
    dark_current=0.01, # e-/pixel/s
    saturation_adu=65_535,
    pixel_scale=0.5,   # arcsec/pixel
)


@pytest.fixture
def camera() -> CameraParams:
    return CAMERA


@pytest.fixture
def saturated_image() -> np.ndarray:
    return np.full((256, 256), CAMERA.saturation_adu, dtype=np.float32)


@pytest.fixture
def sky_dominated_image() -> np.ndarray:
    """Realistic sky-dominated image: sky signal >> RON, no saturation."""
    rng = np.random.default_rng(42)
    sky_adu = 800.0
    return rng.poisson(sky_adu, size=(256, 256)).astype(np.float32)


@pytest.fixture
def ron_dominated_image() -> np.ndarray:
    """Very faint image: sky << RON, instrument-noise dominated."""
    rng = np.random.default_rng(42)
    sky_adu = 2.0   # << RON of 5 e-
    return rng.poisson(sky_adu, size=(256, 256)).astype(np.float32)


# ---------------------------------------------------------------------------
# Fixture image from a real telescope exposure (for smoke-testing the analyzer
# against actual data without needing a live camera).
# ---------------------------------------------------------------------------

# Point this at whatever example FITS files you have on disk.
# Tests that use this fixture are skipped automatically when the file is absent.
import os
EXAMPLE_FITS = os.environ.get("EXAMPLE_FITS_PATH", "")


@pytest.fixture
def example_fits_path():
    if not EXAMPLE_FITS or not os.path.exists(EXAMPLE_FITS):
        pytest.skip("Set EXAMPLE_FITS_PATH env var to a real FITS file to run this test.")
    return EXAMPLE_FITS
