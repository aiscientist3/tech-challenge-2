"""Databricks dbutils resolution for notebooks, jobs, and local dev."""

from __future__ import annotations

from typing import Any


def get_dbutils() -> Any | None:
    """Return Databricks dbutils when running inside a cluster or notebook."""
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-untyped]
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is not None:
            return DBUtils(spark)
    except (ImportError, AttributeError, RuntimeError):
        pass

    try:
        import IPython  # type: ignore[import-untyped]

        return IPython.get_ipython().user_ns.get("dbutils")
    except Exception:
        return None
