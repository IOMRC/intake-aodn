from . import _version
__version__ = _version.get_versions()['version']

import logging
logger = logging.getLogger('intake-aodn')

import os
import warnings
import intake  # Import this first to avoid circular imports during discovery.
from .drivers import RefZarrStackSource

# here = os.path.abspath(os.path.dirname(__file__))
# aodn_cat_path = os.path.join(here, "catalogs")
# if "INTAKE_PATH" in os.environ.keys(): 
#     os.environ["INTAKE_PATH"] =  f'{os.environ["INTAKE_PATH"]}:{aodn_cat_path}'
# else:
#     os.environ["INTAKE_PATH"] =  f'{aodn_cat_path}'
# cat_path = os.path.join(aodn_cat_path, 'main.yaml')    
# if os.path.exists(cat_path):
#     cat = intake.open_catalog(os.path.join(aodn_cat_path, 'main.yaml'))
# else:
cat_url = "https://github.com/NickMortimer/intake-aodn/main/intake_aodn/catalogs/main.yaml"
#https://raw.githubusercontent.com/IOMRC/intake-aodn
#
    # warnings.warn(f'Local catalog not found, loading catalog from github {cat_url}')


