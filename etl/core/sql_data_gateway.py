"""SqlDataGateway - Extractor de datos desde SQL Server.

Ejecuta consultas SQL definidas en archivos .sql contra
la base de datos (localdb)\\MSSQLLocalDB usando pyodbc.
"""

import asyncio
from pathlib import Path
from typing import Any

import pyodbc

from core.data_source_adapter import DataSourceAdapter
from core.trace_manager import TraceManager


class SqlDataGateway(DataSourceAdapter):
    """Ejecuta consultas SQL y extrae datos desde SQL Server."""

    def __init__(
        self,
        connection_string: str,
        sql_folder: str,
        trace: TraceManager,
    ):
        self._connection_string = connection_string
        self._sql_folder = Path(sql_folder)
        self._trace = trace

    def get_source_name(self) -> str:
        return "SqlDataGateway (SQL Server)"

    async def extract(self) -> list[dict[str, Any]]:
        """Ejecuta todos los .sql de la carpeta database/ y retorna resultados."""
        self._trace.start_timer("sql_extraction")
        all_records: list[dict[str, Any]] = []

        sql_files = list(self._sql_folder.glob("*.sql"))
        if not sql_files:
            self._trace.warning(f"No se encontraron archivos SQL en {self._sql_folder}")
            self._trace.stop_timer("sql_extraction")
            return all_records

        for sql_file in sql_files:
            try:
                query = sql_file.read_text(encoding="utf-8").strip()
                if not query:
                    self._trace.warning(f"Archivo SQL vacío: {sql_file.name}")
                    continue

                records = await asyncio.to_thread(self._execute_query, query, sql_file.name)
                self._trace.info(
                    f"SQL '{sql_file.name}': {len(records)} registros extraídos"
                )
                self._trace.increment("sql_rows_read", len(records))
                all_records.extend(records)
            except Exception as e:
                self._trace.error(f"Error ejecutando '{sql_file.name}'", e)

        self._trace.stop_timer("sql_extraction")
        return all_records

    async def validate(self, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Los datos de SQL ya vienen tipados; solo filtra nulos críticos."""
        valid = [r for r in data if r.get("_source_query")]
        self._trace.info(f"SQL: {len(valid)}/{len(data)} registros válidos")
        return valid

    # ── Interno ─────────────────────────────────────────────────
    def _execute_query(self, query: str, source_name: str) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        try:
            conn = pyodbc.connect(self._connection_string, timeout=30)
            cursor = conn.cursor()

            # Ejecutar cada statement separado por GO
            statements = self._split_statements(query)
            for stmt in statements:
                stmt = stmt.strip()
                if not stmt:
                    continue
                try:
                    cursor.execute(stmt)
                    if cursor.description:
                        columns = [col[0] for col in cursor.description]
                        for row in cursor.fetchall():
                            record = dict(zip(columns, row))
                            record["_source_query"] = source_name
                            records.append(record)
                    conn.commit()
                except pyodbc.Error as e:
                    self._trace.warning(f"Statement falló en '{source_name}': {e}")
                    conn.rollback()

            cursor.close()
            conn.close()
        except pyodbc.Error as e:
            self._trace.error(f"Conexión SQL falló", e)
        return records

    @staticmethod
    def _split_statements(sql_text: str) -> list[str]:
        """Divide un script SQL por la directiva GO."""
        import re
        return re.split(r"^\s*GO\s*$", sql_text, flags=re.MULTILINE | re.IGNORECASE)
