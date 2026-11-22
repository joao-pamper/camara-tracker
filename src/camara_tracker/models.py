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
