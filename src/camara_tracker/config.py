"""Configuration defaults for camara-tracker."""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Runtime settings for the Câmara API client."""

    base_url: str = "https://dadosabertos.camara.leg.br/api/v2"
    timeout_s: float = 30.0
    items_per_page: int = 100
    max_retries: int = 5
    backoff_base_s: float = 1.0
    backoff_max_s: float = 30.0
    min_interval_s: float = 0.2
    user_agent: str = "camara-tracker/0.1"
    data_dir: Path = Path("data")
    legislatura: int = 57
