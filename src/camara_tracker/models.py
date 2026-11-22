"""Pydantic models for validated API data."""

from pydantic import BaseModel, ConfigDict


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
