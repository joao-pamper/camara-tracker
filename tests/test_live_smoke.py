"""Opt-in live smoke test against the public Câmara API."""

import pytest

from camara_tracker.api import CamaraClient


@pytest.mark.live
def test_live_deputados() -> None:
    """Fetch one page of current deputies and verify item ids are present."""
    with CamaraClient() as client:
        pages = client.iter_pages(
            "deputados",
            {"idLegislatura": 57},
            max_pages=1,
        )
        items = [item for page in pages for item in page]

    assert any("id" in item for item in items)
