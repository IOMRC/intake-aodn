import intake  # Import this first to avoid circular imports during discovery.
from .drivers import RefZarrStackSource

import logging
import warnings
logger = logging.getLogger('intake-aodn')

import os
import intake

here = os.path.abspath(os.path.dirname(__file__))
aodn_cat_path = os.path.join(here, "catalogs")
if "INTAKE_PATH" in os.environ.keys(): 
    os.environ["INTAKE_PATH"] =  f'{os.environ["INTAKE_PATH"]}:{aodn_cat_path}'
else:
    os.environ["INTAKE_PATH"] =  f'{aodn_cat_path}'
cat = intake.open_catalog(os.path.join(aodn_cat_path, 'main.yaml'))
