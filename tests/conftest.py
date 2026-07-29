"""
Global Pytest Configuration.
Injects Python 3.12 compatibility patches globally before any module imports.
"""

import sys
import types

# --- Python 3.12 Compatibility Patch for Cassandra Driver ---
if "asyncore" not in sys.modules:
    asyncore_mock = types.ModuleType("asyncore")
    class DummyDispatcher:
        pass
    asyncore_mock.dispatcher = DummyDispatcher
    sys.modules['asyncore'] = asyncore_mock
# -------------------------------------------------------------
