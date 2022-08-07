#!/usr/bin/env python3

# Implemented by calling a limited subset of functions from main.py
# Potentially to be extended in the future to load other ref data as well
# Eventually needs to be built into automated ETL - ASN ref data should be
# updated every week as part of the ETL processing.

from main import *

config = load_config(rpath('../configs/config.json'))
loader = LoadToRDS(config)
loader.load_ref_data_rds()
