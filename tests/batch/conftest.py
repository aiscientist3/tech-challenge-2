"""Shared fixtures for batch ingestion tests."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingestion.batch.config import IngestionRunConfig, build_source_configs
from ingestion.batch.sources import SOURCE_REGISTRY

TEST_BUCKET = "test-bucket"
SOURCE_CONFIGS = build_source_configs(TEST_BUCKET)


@pytest.fixture
def run_config() -> IngestionRunConfig:
    return IngestionRunConfig(
        years=[2023, 2024],
        sources=list(SOURCE_CONFIGS.keys()),
        batch_id="test-batch-001",
        row_limit=1000,
        overwrite=True,
    )


@pytest.fixture
def mock_bq_client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def source_instances(mock_bq_client: MagicMock, run_config: IngestionRunConfig) -> dict:
    return {
        name: cls(
            client=mock_bq_client,
            run_config=run_config,
            source_config=SOURCE_CONFIGS[name],
        )
        for name, cls in SOURCE_REGISTRY.items()
    }
