"""Image analysis: numpy array → ExposureAssessment.

Pure function — no I/O, no side effects.
Input: a 2-D array in ADU + camera parameters + exposure time.
Output: an ExposureAssessment.

This is the easiest unit to TDD: feed it synthetic arrays or recorded FITS data.
"""

import numpy as np

from .models import CameraParams, ExposureAssessment


def analyze_image(
    image: np.ndarray,
    exptime: float,
    params: CameraParams,
) -> ExposureAssessment:
    """Assess exposure quality from a single image.

    Args:
        image:   2-D pixel array in ADU.
        exptime: Duration of this exposure in seconds.
        params:  Camera characterisation (gain, RON, dark current, saturation).

    Returns:
        ExposureAssessment describing whether the image is good.
    """
    raise NotImplementedError
