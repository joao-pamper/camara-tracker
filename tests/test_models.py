"""Tests for raw bronze API models."""

import pytest
from pydantic import ValidationError

from camara_tracker.models import Deputado, Despesa, Partido, Votacao, Voto


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


def test_despesa_parses_verified_row() -> None:
    """A verified despesa response row parses with raw field names."""
    row = Despesa.model_validate(
        {
            "ano": 2024,
            "mes": 1,
            "tipoDespesa": "FORNECIMENTO DE ALIMENTAÇÃO DO PARLAMENTAR",
            "codDocumento": "7671013",
            "tipoDocumento": "Nota Fiscal",
            "codTipoDocumento": 0,
            "dataDocumento": "2024-01-01T00:00:00",
            "numDocumento": "008742",
            "valorDocumento": 103.8,
            "urlDocumento": "https://example.test/7671013.pdf",
            "nomeFornecedor": "SAL E BRASA",
            "cnpjCpfFornecedor": "05956940000253",
            "valorLiquido": -103.8,
            "valorGlosa": 0.0,
            "numRessarcimento": "",
            "codLote": 2004391,
            "parcela": 0,
        }
    )

    assert row.ano == 2024
    assert row.codDocumento == "7671013"
    assert row.codTipoDocumento == 0
    assert row.valorLiquido == -103.8
    assert row.numRessarcimento == ""


def test_despesa_accepts_nulls_and_ignores_extras() -> None:
    """Despesa fields are optional and unknown API fields are ignored."""
    row = Despesa.model_validate({"ano": None, "futureField": "ignored"})

    assert row.mes is None
    assert row.valorLiquido is None
    assert not hasattr(row, "futureField")


@pytest.mark.parametrize("model", [Deputado, Partido])
def test_missing_id_fails(model: type[Deputado] | type[Partido]) -> None:
    """Both models require the raw id field."""
    with pytest.raises(ValidationError):
        model.model_validate({"nome": "sem id"})


def test_votacao_parses_verified_row() -> None:
    """A verified votação row parses with its raw string id and raw field names."""
    row = Votacao.model_validate(
        {
            "id": "2390234-8",
            "uri": "https://dadosabertos.camara.leg.br/api/v2/votacoes/2390234-8",
            "data": "2024-03-05",
            "dataHoraRegistro": "2024-03-05T19:07:46",
            "siglaOrgao": "PLEN",
            "uriOrgao": "https://dadosabertos.camara.leg.br/api/v2/orgaos/180",
            "uriEvento": "https://dadosabertos.camara.leg.br/api/v2/eventos/71957",
            "proposicaoObjeto": "REQ 3280/2023",
            "uriProposicaoObjeto": "https://dadosabertos.camara.leg.br/api/v2/proposicoes/2390234",
            "descricao": "Aprovado, por unanimidade.",
            "aprovacao": 1,
            "futureField": "ignored",
        }
    )

    assert row.id == "2390234-8"
    assert isinstance(row.id, str)
    assert row.siglaOrgao == "PLEN"
    assert row.aprovacao == 1
    assert not hasattr(row, "futureField")


def test_votacao_accepts_all_optional_fields_missing() -> None:
    """Only the votação id is required."""
    row = Votacao.model_validate({"id": 42})

    assert row.id == 42
    assert row.data is None
    assert row.siglaOrgao is None


def test_voto_flattens_nested_deputado() -> None:
    """The nested deputado_ object is promoted to flat bronze columns."""
    row = Voto.model_validate(
        {
            "tipoVoto": "Sim",
            "dataRegistroVoto": "2024-03-12T17:56:06",
            "deputado_": {
                "id": 166402,
                "nome": "Magda Mofatto",
                "siglaPartido": "PRD",
                "siglaUf": "GO",
                "idLegislatura": 57,
                "email": "dep@example.test",
            },
        }
    )

    assert row.deputado_id == 166402
    assert row.deputado_siglaPartido == "PRD"
    assert row.deputado_siglaUf == "GO"
    assert row.tipoVoto == "Sim"
    assert row.dataRegistroVoto == "2024-03-12T17:56:06"
    assert not hasattr(row, "deputado_")


def test_voto_requires_deputado_id() -> None:
    """A nested deputado_ object without id fails validation."""
    with pytest.raises(ValidationError):
        Voto.model_validate({"tipoVoto": "Sim", "deputado_": {"nome": "Sem id"}})
