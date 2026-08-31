"""
Scenario registry for the 4-test suite.
"""

from __future__ import annotations

from ..framework.config import TestConfig
from .tc01_sanity       import TC01_Sanity
from .tc02_v3_stress    import TC02_NFSv3_Stress
from .tc03_v4_stress    import TC03_NFSv4_Stress
from .tc04_mixed_stress import TC04_Mixed_Stress
from .mode              import RunMode

ALL_SCENARIOS = [TC01_Sanity, TC02_NFSv3_Stress, TC03_NFSv4_Stress, TC04_Mixed_Stress]
SCENARIO_MAP  = {cls.SCENARIO_ID: cls for cls in ALL_SCENARIOS}


def get_scenario(scenario_id: str, config: TestConfig, mode: str = RunMode.NORMAL):
    """Return an instantiated scenario by ID (TC01–TC04)."""
    cls = SCENARIO_MAP[scenario_id.upper()]
    return cls(config, mode=mode)
