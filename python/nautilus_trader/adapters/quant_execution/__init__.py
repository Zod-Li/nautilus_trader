# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
# -------------------------------------------------------------------------------------------------
"""
In-memory O(1) PlanGate admission engine and async write-behind telemetry bindings.
"""

from __future__ import annotations

from nautilus_trader._fixup import fixup_module_names
from nautilus_trader._libnautilus.quant_execution import *  # noqa: F403


__all__ = [
    "RustPlanGateDecision",
    "RustPlanGateEngine",
    "RustPlanGateOrderRequest",
    "RustPlanItemEnvelope",
]

fixup_module_names(globals(), __name__)
del fixup_module_names
