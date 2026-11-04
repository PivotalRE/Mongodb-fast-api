"""Lets Railway's old start command (`uvicorn api.main:app ...`) start the property agent.

The service's dashboard may still hold the start command of the older MongoDB API.
This module gives that command the agent app, so the service works either way.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "agent_app"))

from service import app_factory  # noqa: E402

app = app_factory()
