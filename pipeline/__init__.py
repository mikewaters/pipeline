"""
Pipeline

This package (ab)uses the registry pattern, so it it is critical that
modules using registries are imported as early as possible
"""


import pipeline.signals
import pipeline.workspace
import pipeline.command


