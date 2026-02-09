#
# Copyright (c) 2024, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Flag to track if dask-expr is being used
# With dask 2025.1.0+, dask-expr is the only backend
_DASK_EXPR_ENABLED = False

try:
    import dask
    from packaging.version import parse as parse_version

    _dask_version = parse_version(dask.__version__)

    # For dask >= 2025.1.0, dask-expr is mandatory (legacy removed)
    # For dask >= 2024.3.0, dask-expr is available but optional
    if _dask_version >= parse_version("2025.1.0"):
        # dask-expr is now mandatory - no configuration needed
        _DASK_EXPR_ENABLED = True
    elif _dask_version >= parse_version("2024.3.0"):
        # Check if query-planning is enabled
        import sys

        # Try to disable query-planning for older versions
        # where it's still optional
        try:
            dask.config.set(
                {
                    "dataframe.query-planning": False,
                    "dataframe.convert-string": False,
                }
            )
        except Exception:
            pass

        import dask.dataframe as dd

        if hasattr(dd, "DASK_EXPR_ENABLED"):
            _DASK_EXPR_ENABLED = dd.DASK_EXPR_ENABLED
        else:
            _DASK_EXPR_ENABLED = "dask_expr" in sys.modules
    else:
        # Older versions - disable query planning if available
        try:
            dask.config.set(
                {
                    "dataframe.query-planning": False,
                    "dataframe.convert-string": False,
                }
            )
        except Exception:
            pass
        _DASK_EXPR_ENABLED = False

except ImportError:
    dask = None


def validate_dask_configs():
    """Central check for dask configuration.

    Note: With dask >= 2025.1.0, dask-expr is the mandatory backend.
    This function now only checks for the convert-string config option
    on older dask versions.
    """
    if dask is None:
        return

    from packaging.version import parse as parse_version

    _dask_version = parse_version(dask.__version__)

    # For dask >= 2025.1.0, dask-expr is mandatory, no need to check
    if _dask_version >= parse_version("2025.1.0"):
        return

    # For older versions, check if convert-string is enabled
    try:
        if dask.config.get("dataframe.convert-string", False):
            import warnings

            warnings.warn(
                "Automatic string conversion is enabled in Dask. "
                "This may cause unexpected behavior with Merlin. "
                "Consider disabling with: "
                "dask.config.set({'dataframe.convert-string': False})"
            )
    except Exception:
        pass
