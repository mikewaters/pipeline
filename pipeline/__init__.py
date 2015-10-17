"""
Pipeline

"""
# This package (ab)uses the registry pattern, so it it is critical that
# modules using registries are imported as early as possible
import pipeline.actions
import pipeline.command
import pipeline.signals
import pipeline.workspace
import pipeline.criteria
# shortcut imports for common abstractions
from pipeline.actions import TaskAction, action, ActionHook
from pipeline.context import BuildContext
from pipeline.executor import Pipeline
from pipeline.workspace import Workspace
