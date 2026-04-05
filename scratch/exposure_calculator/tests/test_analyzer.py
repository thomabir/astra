"""Tests for analyze_image — fully offline, no hardware required.

All tests use either synthetic numpy arrays (constructed here) or
real example FITS files injected via the EXAMPLE_FITS_PATH env var.
No camera, no network, no astra observatory instance needed.
"""

import numpy as np
import pytest

from exposure_calculator.analyzer import analyze_image


# ---------------------------------------------------------------------------
# Saturation
# ---------------------------------------------------------------------------

def test_fully_saturated_image_is_detected(camera, saturated_image):
    result = analyze_image(saturated_image, exptime=300.0, params=camera)
    assert result.saturated
    assert not result.is_good


def test_clean_image_is_not_saturated(camera, sky_dominated_image):
    result = analyze_image(sky_dominated_image, exptime=60.0, params=camera)
    assert not result.saturated


def test_saturation_fraction_is_zero_for_clean_image(camera, sky_dominated_image):
    result = analyze_image(sky_dominated_image, exptime=60.0, params=camera)
    assert result.saturation_fraction == pytest.approx(0.0)


def test_saturation_fraction_is_one_for_full_saturation(camera, saturated_image):
    result = analyze_image(saturated_image, exptime=300.0, params=camera)
    assert result.saturation_fraction == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Noise regime
# ---------------------------------------------------------------------------

def test_sky_dominated_image_passes_noise_check(camera, sky_dominated_image):
    result = analyze_image(sky_dominated_image, exptime=60.0, params=camera)
    assert not result.noise_dominated
    assert result.noise_ratio < 1.0


def test_ron_dominated_image_fails_noise_check(camera, ron_dominated_image):
    result = analyze_image(ron_dominated_image, exptime=1.0, params=camera)
    assert result.noise_dominated
    assert result.noise_ratio >= 1.0


# ---------------------------------------------------------------------------
# is_good combines both criteria
# ---------------------------------------------------------------------------

def test_good_image_is_sky_dominated_and_unsaturated(camera, sky_dominated_image):
    result = analyze_image(sky_dominated_image, exptime=60.0, params=camera)
    assert result.is_good


def test_saturated_image_is_never_good(camera, saturated_image):
    result = analyze_image(saturated_image, exptime=300.0, params=camera)
    assert not result.is_good


# ---------------------------------------------------------------------------
# Background estimate
# ---------------------------------------------------------------------------

def test_sky_background_is_close_to_injected_value(camera):
    rng = np.random.default_rng(0)
    sky_adu = 500.0
    image = rng.poisson(sky_adu, size=(256, 256)).astype(np.float32)
    result = analyze_image(image, exptime=60.0, params=camera)
    assert abs(result.sky_background_adu - sky_adu) < 20  # within ~4%


# ---------------------------------------------------------------------------
# Smoke test against real telescope data
# (skipped automatically when EXAMPLE_FITS_PATH is not set)
# ---------------------------------------------------------------------------

def test_real_image_produces_valid_assessment(camera, example_fits_path):
    import astropy.io.fits as fits
    image = fits.open(example_fits_path)[0].data.astype(np.float32)
    result = analyze_image(image, exptime=60.0, params=camera)
    assert 0.0 <= result.saturation_fraction <= 1.0
    assert result.sky_background_adu >= 0.0
    assert result.noise_ratio >= 0.0
