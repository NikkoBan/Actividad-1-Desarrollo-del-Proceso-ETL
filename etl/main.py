"""
ETL Worker Service - Punto de entrada principal.

Simula un Worker Service que:
1. Carga configuración desde config.json y .env
2. Inicializa todos los componentes (adaptadores, sink, trace, dw_loader)
3. Ejecuta la extracción desde CSV, SQL Server y API REST
4. Valida datos con DTOs Pydantic
5. Almacena resultados en staging/ (archivos)
6. Carga datos en staging del DW (tablas SQL)
7. Ejecuta ETL al Data Warehouse (dimensiones + hechos)
8. Registra logs y métricas de ejecución
"""

import asyncio
import signal
import sys
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent))

from config import Settings
from core import (
    FlatFileReader,
    SqlDataGateway,
    HttpDataCollector,
    DataSinkManager,
    TraceManager,
    DwLoader,
)


class EtlWorker:
    """Orquestador principal del proceso ETL."""

    def __init__(self):
        self._settings = Settings()
        self._trace = TraceManager(
            self._settings.logs_folder,
            self._settings.log_level,
        )
        self._sink = DataSinkManager(
            self._settings.staging_folder,
            self._trace,
        )
        self._dw = DwLoader(
            self._settings.dw_connection_string,
            self._trace,
        )
        self._running = True

        # Adaptadores de extracción
        self._extractors = {
            "csv": FlatFileReader(
                self._settings.csv_folder,
                self._trace,
            ),
            "sql": SqlDataGateway(
                self._settings.db_connection_string,
                str(Path(__file__).parent / "database"),
                self._trace,
            ),
            "api": HttpDataCollector(
                self._settings.api_base_url,
                self._settings.api_endpoints,
                self._trace,
                self._settings.api_retry_max,
                self._settings.api_retry_delay,
            ),
        }

    async def run(self) -> None:
        """Ejecuta el ciclo completo del ETL."""
        self._trace.start_timer("etl_total")
        self._trace.info("=" * 60)
        self._trace.info("🚀  INICIO DEL PROCESO ETL")
        self._trace.info("=" * 60)

        try:
            # ── FASE 1: Extracción ──────────────────────────────
            self._trace.info("── FASE 1: EXTRACCIÓN ──")
            extracted_data: dict[str, list] = {}

            for name, extractor in self._extractors.items():
                if not self._running:
                    break
                self._trace.info(f"Extrayendo desde: {extractor.get_source_name()}")
                try:
                    data = await extractor.extract()
                    extracted_data[name] = data
                    self._trace.info(f"  → {len(data)} registros extraídos de '{name}'")
                except Exception as e:
                    self._trace.error(f"Error en extracción '{name}'", e)
                    extracted_data[name] = []

            # ── FASE 2: Validación / Transformación ─────────────
            self._trace.info("── FASE 2: VALIDACIÓN ──")
            validated_data: dict[str, list] = {}

            for name, extractor in self._extractors.items():
                if not self._running:
                    break
                raw = extracted_data.get(name, [])
                if raw:
                    try:
                        valid = await extractor.validate(raw)
                        validated_data[name] = valid
                        self._trace.info(
                            f"  → '{name}': {len(valid)}/{len(raw)} registros válidos"
                        )
                    except Exception as e:
                        self._trace.error(f"Error validando '{name}'", e)
                        validated_data[name] = raw
                else:
                    validated_data[name] = []

            # ── FASE 3: Carga en archivos staging ───────────────
            self._trace.info("── FASE 3: CARGA EN STAGING (archivos) ──")

            csv_data = validated_data.get("csv", [])
            if csv_data:
                await self._sink.save_grouped(csv_data, "_source_file", "csv")

            sql_data = validated_data.get("sql", [])
            if sql_data:
                await self._sink.save_grouped(sql_data, "_source_query", "sql")

            api_data = validated_data.get("api", [])
            if api_data:
                await self._sink.save_grouped(api_data, "_source_endpoint", "api")

            await self._sink.save_as_json(
                [
                    {
                        "csv_records": len(csv_data),
                        "sql_records": len(sql_data),
                        "api_records": len(api_data),
                        "total_records": len(csv_data) + len(sql_data) + len(api_data),
                    }
                ],
                "etl_summary",
            )

            # ── FASE 4: Carga al Data Warehouse ────────────────
            self._trace.info("── FASE 4: CARGA AL DATA WAREHOUSE ──")

            # Clasificar CSV por archivo fuente para staging DB
            csv_by_file = self._group_by_key(csv_data, "_source_file")

            for file_key, records in csv_by_file.items():
                lower = file_key.lower()
                if "customer" in lower:
                    await self._dw.load_staging_clientes(records, "CSV")
                elif "product" in lower:
                    await self._dw.load_staging_productos(records, "CSV")
                elif "order_detail" in lower:
                    await self._dw.load_staging_detalle(records, "CSV")
                elif "order" in lower:
                    await self._dw.load_staging_ventas(records, "CSV")

            # Cargar comentarios de la API
            if api_data:
                comments = [r for r in api_data if r.get("_source_endpoint", "") == "comments"]
                if comments:
                    await self._dw.load_staging_api_comentarios(comments)

            # Ejecutar SP que transforma staging → dimensiones → hechos
            await self._dw.execute_etl_to_dw()

        except Exception as e:
            self._trace.error("Error fatal en el proceso ETL", e)
        finally:
            self._trace.stop_timer("etl_total")
            self._trace.print_summary()
            self._trace.info("🏁  PROCESO ETL FINALIZADO")

    def stop(self) -> None:
        """Señal de parada para el worker."""
        self._trace.info("Señal de parada recibida. Finalizando...")
        self._running = False

    @staticmethod
    def _group_by_key(records: list[dict], key: str) -> dict[str, list[dict]]:
        """Agrupa registros por el valor de una clave interna."""
        groups: dict[str, list[dict]] = {}
        for r in records:
            k = r.get(key, "unknown")
            groups.setdefault(k, []).append(r)
        return groups


def main() -> None:
    """Punto de entrada: configura signals y ejecuta el worker."""
    worker = EtlWorker()

    def signal_handler(sig, frame):
        worker.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    asyncio.run(worker.run())


if __name__ == "__main__":
    main()
