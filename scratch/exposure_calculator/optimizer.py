"""Exposure optimiser: iterates test exposures until the image is good.

Depends only on the ImageSource abstraction and the pure analyzer function.
Does not know about cameras, FITS files, schedules, or astra internals.
"""

import logging

from .analyzer import analyze_image
from .interfaces import ImageSource
from .models import CameraParams, ExposureAssessment

logger = logging.getLogger(__name__)


class ExposureOptimizer:
    """Finds an optimal exposure time by taking test exposures.

    Usage:
        optimizer = ExposureOptimizer(source, camera_params, initial_exptime=60.0)
        best_exptime = optimizer.optimize()

    For offline TDD: inject a MockImageSource or a FixtureImageSource that
    returns pre-recorded arrays without touching any hardware.
    """

    def __init__(
        self,
        image_source: ImageSource,
        camera_params: CameraParams,
        initial_exptime: float,
        max_iterations: int = 5,
    ) -> None:
        self.source = image_source
        self.params = camera_params
        self.initial_exptime = initial_exptime
        self.max_iterations = max_iterations

    def optimize(self) -> float:
        """Return a good exposure time, taking up to max_iterations test exposures."""
        exptime = self.initial_exptime
        for i in range(self.max_iterations):
            image = self.source.capture(exptime)
            assessment = analyze_image(image, exptime, self.params)
            logger.info(
                f"Iteration {i+1}: exptime={exptime:.1f}s  "
                f"saturated={assessment.saturated}  "
                f"noise_ratio={assessment.noise_ratio:.2f}"
            )
            if assessment.is_good:
                return exptime
            exptime = _suggest_next_exptime(exptime, assessment)
        logger.warning("Max iterations reached; returning best guess.")
        return exptime


def _suggest_next_exptime(
    current_exptime: float, assessment: ExposureAssessment
) -> float:
    """Pure function: given the current assessment, propose the next exptime.

    This is deliberately separate so it can be unit-tested without any I/O.
    """
    raise NotImplementedError
