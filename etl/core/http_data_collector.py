"""HttpDataCollector - Extractor de datos desde APIs REST.

Consume endpoints configurados con retry automático,
valida respuestas con DTOs Pydantic y maneja errores HTTP.
"""

import asyncio
from typing import Any

import aiohttp
from pydantic import ValidationError

from core.data_source_adapter import DataSourceAdapter
from core.trace_manager import TraceManager
from dto import UserDTO, PostDTO, CommentDTO


_ENDPOINT_DTO_MAP: dict[str, type] = {
    "/users": UserDTO,
    "/posts": PostDTO,
    "/comments": CommentDTO,
}


class HttpDataCollector(DataSourceAdapter):
    """Consume APIs REST con retry automático y validación Pydantic."""

    def __init__(
        self,
        base_url: str,
        endpoints: dict[str, str],
        trace: TraceManager,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ):
        self._base_url = base_url.rstrip("/")
        self._endpoints = endpoints
        self._trace = trace
        self._max_retries = max_retries
        self._retry_delay = retry_delay

    def get_source_name(self) -> str:
        return "HttpDataCollector (API REST)"

    async def extract(self) -> list[dict[str, Any]]:
        """Consume todos los endpoints configurados en paralelo."""
        self._trace.start_timer("api_extraction")
        all_records: list[dict[str, Any]] = []

        async with aiohttp.ClientSession() as session:
            tasks = [
                self._fetch_endpoint(session, name, path)
                for name, path in self._endpoints.items()
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    self._trace.error("Error en extracción API", result)
                elif isinstance(result, list):
                    all_records.extend(result)

        self._trace.stop_timer("api_extraction")
        return all_records

    async def validate(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Valida cada registro contra su DTO Pydantic correspondiente."""
        self._trace.start_timer("api_validation")
        valid_records: list[dict[str, Any]] = []

        for record in data:
            endpoint = record.get("_source_endpoint", "")
            dto_class = _ENDPOINT_DTO_MAP.get(endpoint)

            if dto_class is None:
                valid_records.append(record)
                continue

            try:
                clean = {k: v for k, v in record.items() if not k.startswith("_")}
                dto = dto_class(**clean)
                validated = dto.model_dump(by_alias=False)
                validated["_source_endpoint"] = endpoint
                valid_records.append(validated)
                self._trace.increment("api_rows_valid")
            except ValidationError as e:
                self._trace.warning(f"Dato API inválido ({endpoint}): {e.error_count()} errores")
                self._trace.increment("api_rows_invalid")

        self._trace.stop_timer("api_validation")
        return valid_records

    # ── Interno ─────────────────────────────────────────────────
    async def _fetch_endpoint(
        self,
        session: aiohttp.ClientSession,
        name: str,
        path: str,
    ) -> list[dict[str, Any]]:
        """Consume un endpoint con sistema de retry automático."""
        url = f"{self._base_url}{path}"

        for attempt in range(1, self._max_retries + 1):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if isinstance(data, list):
                            for item in data:
                                item["_source_endpoint"] = path
                            self._trace.info(
                                f"API '{name}' ({path}): {len(data)} registros"
                            )
                            self._trace.increment("api_rows_read", len(data))
                            return data
                        return []
                    else:
                        self._trace.warning(
                            f"API '{name}' respondió {resp.status} (intento {attempt}/{self._max_retries})"
                        )
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                self._trace.warning(
                    f"API '{name}' falló (intento {attempt}/{self._max_retries}): {e}"
                )

            if attempt < self._max_retries:
                await asyncio.sleep(self._retry_delay * attempt)

        self._trace.error(f"API '{name}' falló después de {self._max_retries} intentos")
        return []
