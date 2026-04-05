"""Abstract interfaces — the seams that make offline TDD possible.

Everything above these interfaces (analysis, optimisation, schedule rewriting)
is testable with synthetic data or recorded example images.
Everything below them (Alpaca camera, live Gaia DB) is integration-tested only.
"""

from abc import ABC, abstractmethod

import numpy as np


class ImageSource(ABC):
    """Anything that can produce a 2-D image array at a given exposure time."""

    @abstractmethod
    def capture(self, exptime: float) -> np.ndarray:
        """Return a 2-D array of pixel values in ADU."""
        ...


class CatalogSource(ABC):
    """Anything that can answer photometry questions about a sky position."""

    @abstractmethod
    def brightest_star_magnitude(self, ra: float, dec: float, fov_deg: float) -> float:
        """Return the G-band magnitude of the brightest star in the field."""
        ...
