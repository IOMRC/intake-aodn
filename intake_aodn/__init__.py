import os
import intake  # Import this first to avoid circular imports during discovery.
from .drivers import RefZarrStackSource

import logging
logger = logging.getLogger('intake-aodn')

cat = intake.open_catalog("https://raw.githubusercontent.com/IOMRC/intake-aodn/main/intake_aodn/catalogs/main.yaml")

from . import _version
__version__ = _version.get_versions()['version']
