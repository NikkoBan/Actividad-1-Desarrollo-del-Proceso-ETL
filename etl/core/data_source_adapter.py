"""DataSourceAdapter - Interfaz base (ABC) para cualquier fuente de datos.

Principio: Interface Segregation (SOLID - I)
Cada fuente de datos implementa esta interfaz, permitiendo intercambiabilidad
y desacoplamiento entre la lógica de extracción y la fuente concreta.
"""

from abc import ABC, abstractmethod
from typing import Any


class DataSourceAdapter(ABC):
    """Contrato base para todos los extractores de datos."""

    @abstractmethod
    async def extract(self) -> list[dict[str, Any]]:
        """Extrae datos de la fuente y retorna una lista de diccionarios."""

    @abstractmethod
    async def validate(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Valida los datos extraídos y retorna solo los válidos."""

    @abstractmethod
    def get_source_name(self) -> str:
        """Retorna el nombre identificador de la fuente de datos."""
