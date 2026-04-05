"""Pure data models. No I/O, no hardware, no astra imports."""

from dataclasses import dataclass


@dataclass(frozen=True)
class CameraParams:
    gain: float          # e-/ADU
    ron: float           # electrons RMS
    dark_current: float  # e-/pixel/s
    saturation_adu: int
    pixel_scale: float   # arcsec/pixel


@dataclass(frozen=True)
class ExposureAssessment:
    """Result of analysing a single test image."""
    saturated: bool              # any pixels at/above saturation (excluding known hot pixels)
    noise_dominated: bool        # N_inst >= N_fundamental
    sky_background_adu: float
    noise_ratio: float           # N_inst / N_fundamental  (want << 1)
    saturation_fraction: float   # fraction of pixels that are saturated

    @property
    def is_good(self) -> bool:
        return not self.saturated and not self.noise_dominated
