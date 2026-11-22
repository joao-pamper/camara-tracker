"""Tests for configuration defaults."""

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from camara_tracker.config import Settings


def test_settings_defaults() -> None:
    """Settings expose the documented client defaults."""
    settings = Settings()

    assert settings.base_url == "https://dadosabertos.camara.leg.br/api/v2"
    assert settings.timeout_s == 30.0
    assert settings.items_per_page == 100
    assert settings.max_retries == 5
    assert settings.backoff_base_s == 1.0
    assert settings.backoff_max_s == 30.0
    assert settings.min_interval_s == 0.2
    assert settings.user_agent == "camara-tracker/0.1"
    assert settings.data_dir == Path("data")
    assert settings.legislatura == 57
    assert settings.first_year == 2023
    assert settings.shard_max_age_hours == 24.0


def test_settings_are_frozen() -> None:
    """Settings cannot be mutated after construction."""
    settings = Settings()

    with pytest.raises(FrozenInstanceError):
        settings.timeout_s = 1.0
