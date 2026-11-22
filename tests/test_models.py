"""Tests for raw bronze API models."""

import pytest
from pydantic import ValidationError

from camara_tracker.models import Deputado, Partido


def test_deputado_parses_valid_row() -> None:
    """A verified deputado response row parses with raw field names."""
    row = Deputado.model_validate(
        {
            "id": 220593,
            "uri": "https://dadosabertos.camara.leg.br/api/v2/deputados/220593",
            "nome": "Abilio Brunini",
            "siglaPartido": "PL",
            "uriPartido": "https://dadosabertos.camara.leg.br/api/v2/partidos/37906",
            "siglaUf": "MT",
            "idLegislatura": 57,
            "urlFoto": "https://www.camara.leg.br/internet/deputado/bandep/220593.jpg",
            "email": None,
        }
    )

    assert row.id == 220593
    assert row.nome == "Abilio Brunini"
    assert row.email is None
    assert row.idLegislatura == 57


def test_deputado_accepts_nulls_and_ignores_extras() -> None:
    """Optional fields may be null and unknown API fields are ignored."""
    row = Deputado.model_validate({"id": 1, "email": None, "futureField": "ignored"})

    assert row.uri is None
    assert row.email is None
    assert not hasattr(row, "futureField")


def test_partido_parses_valid_row() -> None:
    """A verified partido response row parses with raw field names."""
    row = Partido.model_validate(
        {
            "id": 36898,
            "sigla": "AVANTE",
            "nome": "Avante",
            "uri": "https://dadosabertos.camara.leg.br/api/v2/partidos/36898",
        }
    )

    assert row.id == 36898
    assert row.sigla == "AVANTE"
    assert row.nome == "Avante"


@pytest.mark.parametrize("model", [Deputado, Partido])
def test_missing_id_fails(model: type[Deputado] | type[Partido]) -> None:
    """Both models require the raw id field."""
    with pytest.raises(ValidationError):
        model.model_validate({"nome": "sem id"})
