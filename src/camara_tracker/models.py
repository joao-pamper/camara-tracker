"""Pydantic models for validated API data."""

from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator


class Deputado(BaseModel):
    """Raw deputado fields as returned by the API."""

    model_config = ConfigDict(extra="ignore")

    id: int
    uri: str | None = None
    nome: str | None = None
    siglaPartido: str | None = None
    uriPartido: str | None = None
    siglaUf: str | None = None
    idLegislatura: int | None = None
    urlFoto: str | None = None
    email: str | None = None


class DeputadoHistorico(BaseModel):
    """Raw deputy status-history fields as returned by the API."""

    model_config = ConfigDict(extra="ignore")

    id: int
    uri: str | None = None
    nome: str | None = None
    siglaPartido: str | None = None
    uriPartido: str | None = None
    siglaUf: str | None = None
    idLegislatura: int | None = None
    urlFoto: str | None = None
    email: str | None = None
    dataHora: str
    situacao: str | None = None
    condicaoEleitoral: str | None = None
    descricaoStatus: str | None = None


class Proposicao(BaseModel):
    """Raw authored-proposition fields as returned by the API."""

    model_config = ConfigDict(extra="ignore")

    id: int
    uri: str | None = None
    siglaTipo: str | None = None
    codTipo: int | None = None
    numero: int | None = None
    ano: int | None = None
    ementa: str | None = None


class Partido(BaseModel):
    """Raw partido fields as returned by the API."""

    model_config = ConfigDict(extra="ignore")

    id: int
    sigla: str | None = None
    nome: str | None = None
    uri: str | None = None


class Despesa(BaseModel):
    """Raw despesa fields as returned by the API."""

    model_config = ConfigDict(extra="ignore")

    ano: int | None = None
    mes: int | None = None
    tipoDespesa: str | None = None
    codDocumento: str | None = None
    tipoDocumento: str | None = None
    codTipoDocumento: int | None = None
    dataDocumento: str | None = None
    numDocumento: str | None = None
    parcela: int | None = None
    valorDocumento: float | None = None
    valorLiquido: float | None = None
    valorGlosa: float | None = None
    nomeFornecedor: str | None = None
    cnpjCpfFornecedor: str | None = None
    codLote: int | None = None
    urlDocumento: str | None = None
    numRessarcimento: str | None = None


class Votacao(BaseModel):
    """Raw votação fields as returned by the API; ``id`` keeps its API type."""

    model_config = ConfigDict(extra="ignore")

    id: str | int
    uri: str | None = None
    data: str | None = None
    dataHoraRegistro: str | None = None
    siglaOrgao: str | None = None
    uriOrgao: str | None = None
    uriEvento: str | None = None
    proposicaoObjeto: str | None = None
    uriProposicaoObjeto: str | None = None
    descricao: str | None = None
    aprovacao: int | None = None


class Voto(BaseModel):
    """Voto row with the nested ``deputado_`` object flattened for bronze.

    The API returns ``{"tipoVoto": ..., "dataRegistroVoto": ..., "deputado_": {...}}``.
    Validation promotes ``deputado_.id`` to the required ``deputado_id`` and
    ``deputado_.siglaPartido`` / ``deputado_.siglaUf`` to ``deputado_siglaPartido`` /
    ``deputado_siglaUf``. The nested object and all other fields are ignored, while
    ``tipoVoto`` and ``dataRegistroVoto`` are kept exactly as returned.
    """

    model_config = ConfigDict(extra="ignore")

    deputado_id: int
    deputado_siglaPartido: str | None = None
    deputado_siglaUf: str | None = None
    tipoVoto: str | None = None
    dataRegistroVoto: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _flatten_deputado(cls, value: Any) -> Any:
        """Copy nested deputy fields into flat names before field validation."""
        if not isinstance(value, dict):
            return value
        deputado = value.get("deputado_")
        if not isinstance(deputado, dict):
            return value
        flattened = dict(value)
        flattened["deputado_id"] = deputado.get("id")
        flattened["deputado_siglaPartido"] = deputado.get("siglaPartido")
        flattened["deputado_siglaUf"] = deputado.get("siglaUf")
        return flattened
