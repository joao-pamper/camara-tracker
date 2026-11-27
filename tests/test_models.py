"""Tests for raw bronze API models."""

import pytest
from pydantic import ValidationError

from camara_tracker.models import (
    Deputado,
    DeputadoHistorico,
    Despesa,
    Partido,
    Proposicao,
    Votacao,
    Voto,
)


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


@pytest.mark.parametrize("model", [Deputado, Partido, DeputadoHistorico, Proposicao])
def test_missing_id_fails(
    model: type[Deputado] | type[Partido] | type[DeputadoHistorico] | type[Proposicao],
) -> None:
    """All raw bronze models require the API id field."""
    with pytest.raises(ValidationError):
        model.model_validate({"nome": "sem id"})


def test_deputado_historico_parses_verified_row() -> None:
    """A verified histórico row parses with raw field names and ignores extras."""
    row = DeputadoHistorico.model_validate(
        {
            "id": 62881,
            "uri": "https://dadosabertos.camara.leg.br/api/v2/deputados/62881",
            "nome": "Danilo Forte",
            "nomeEleitoral": "DANILO FORTE",
            "siglaPartido": "PP",
            "uriPartido": "https://dadosabertos.camara.leg.br/api/v2/partidos/37903",
            "siglaUf": "CE",
            "idLegislatura": 58,
            "urlFoto": "https://www.camara.leg.br/internet/deputado/bandep/62881.jpg",
            "email": None,
            "dataHora": "2023-02-01T00:00",
            "situacao": None,
            "condicaoEleitoral": None,
            "descricaoStatus": "Nome no início da legislatura",
            "futureField": "ignored",
        }
    )

    assert row.id == 62881
    assert row.dataHora == "2023-02-01T00:00"
    assert row.idLegislatura == 58
    assert row.situacao is None
    assert not hasattr(row, "nomeEleitoral")
    assert not hasattr(row, "futureField")


def test_deputado_historico_requires_data_hora() -> None:
    """A histórico row without dataHora fails validation."""
    with pytest.raises(ValidationError):
        DeputadoHistorico.model_validate({"id": 1})


def test_proposicao_parses_verified_row() -> None:
    """A verified proposição row parses with raw field names and ignores extras."""
    row = Proposicao.model_validate(
        {
            "id": 2418080,
            "uri": "https://dadosabertos.camara.leg.br/api/v2/proposicoes/2418080",
            "siglaTipo": "REQ",
            "codTipo": 609,
            "numero": 282,
            "ano": 2024,
            "ementa": "Requer a criação de uma frente parlamentar.",
            "dataApresentacao": "2024-02-20T13:01",
            "futureField": "ignored",
        }
    )

    assert row.id == 2418080
    assert row.siglaTipo == "REQ"
    assert row.numero == 282
    assert not hasattr(row, "dataApresentacao")
    assert not hasattr(row, "futureField")


def test_proposicao_accepts_nulls_and_ignores_extras() -> None:
    """Proposição optional fields may be null."""
    row = Proposicao.model_validate({"id": 1, "ano": None, "ementa": None})

    assert row.uri is None
    assert row.codTipo is None
    assert row.ano is None


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
