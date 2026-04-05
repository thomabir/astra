"""Root conftest for the scratch directory.

Adds scratch/ to sys.path so that `import exposure_calculator` works
without installing anything.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
