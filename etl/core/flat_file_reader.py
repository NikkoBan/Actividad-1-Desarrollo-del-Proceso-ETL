"""FlatFileReader - Extractor de archivos CSV.

Lee archivos CSV desde una carpeta configurada, valida cada fila
usando DTOs Pydantic y transforma los datos a diccionarios limpios.
"""

import asyncio
import csv
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from core.data_source_adapter import DataSourceAdapter
from core.trace_manager import TraceManager
from dto import CustomerDTO, OrderDTO, ProductDTO, OrderDetailDTO


# Mapeo archivo → DTO para validación automática
_CSV_DTO_MAP: dict[str, type] = {
    "customers.csv": CustomerDTO,
    "orders.csv": OrderDTO,
    "products.csv": ProductDTO,
    "order_details.csv": OrderDetailDTO,
}


class FlatFileReader(DataSourceAdapter):
    """Lee y valida archivos CSV desde el directorio configurado."""

    def __init__(self, csv_folder: str, trace: TraceManager):
        self._csv_folder = Path(csv_folder)
        self._trace = trace

    def get_source_name(self) -> str:
        return "FlatFileReader (CSV)"

    async def extract(self) -> list[dict[str, Any]]:
        """Lee todos los CSV de la carpeta y retorna los registros combinados."""
        self._trace.start_timer("csv_extraction")
        all_records: list[dict[str, Any]] = []

        csv_files = list(self._csv_folder.glob("*.csv"))
        if not csv_files:
            self._trace.warning(f"No se encontraron archivos CSV en {self._csv_folder}")
            self._trace.stop_timer("csv_extraction")
            return all_records

        for csv_file in csv_files:
            try:
                records = await asyncio.to_thread(self._read_csv, csv_file)
                self._trace.info(
                    f"CSV '{csv_file.name}': {len(records)} registros leídos"
                )
                self._trace.increment("csv_rows_read", len(records))
                all_records.extend(records)
            except Exception as e:
                self._trace.error(f"Error leyendo '{csv_file.name}'", e)

        self._trace.stop_timer("csv_extraction")
        return all_records

    async def validate(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Valida cada registro contra su DTO Pydantic correspondiente."""
        self._trace.start_timer("csv_validation")
        valid_records: list[dict[str, Any]] = []

        for record in data:
            source_file = record.get("_source_file", "")
            dto_class = _CSV_DTO_MAP.get(source_file)

            if dto_class is None:
                valid_records.append(record)
                continue

            try:
                clean = {k: v for k, v in record.items() if not k.startswith("_")}
                dto = dto_class(**clean)
                validated = dto.model_dump(by_alias=False)
                validated["_source_file"] = source_file
                valid_records.append(validated)
                self._trace.increment("csv_rows_valid")
            except ValidationError as e:
                self._trace.warning(
                    f"Registro inválido en '{source_file}': {e.error_count()} errores"
                )
                self._trace.increment("csv_rows_invalid")

        self._trace.stop_timer("csv_validation")
        return valid_records

    # ── Interno ─────────────────────────────────────────────────
    def _read_csv(self, file_path: Path) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row["_source_file"] = file_path.name
                records.append(dict(row))
        return records
