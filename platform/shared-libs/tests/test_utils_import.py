"""Basic import contract for track_platform.utils.

Ensures the module can be imported even while individual helpers are
not yet implemented. The previous implementation imported missing
submodules and raised ModuleNotFoundError at import time, breaking any
consumer that merely imported the package.
"""

import importlib
import pytest


def test_utils_module_importable():
    # Should not raise ModuleNotFoundError during import
    module = importlib.import_module("track_platform.utils")
    assert hasattr(module, "validate_schema")


@pytest.mark.parametrize(
    "func_name",
    ["validate_schema", "parse_config", "get_incremental_value", "format_table_name"],
)
def test_stub_functions_raise_not_implemented(func_name):
    module = importlib.import_module("track_platform.utils")
    func = getattr(module, func_name)
    with pytest.raises(NotImplementedError):
        func()

