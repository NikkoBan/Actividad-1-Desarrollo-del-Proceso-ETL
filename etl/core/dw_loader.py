"""DwLoader - Carga de datos al Data Warehouse (VentasAnalisisDB).

Gestiona el flujo completo:
  1. Insertar datos extraídos en tablas de staging
  2. Ejecutar los stored procedures ETL para cargar dimensiones y hechos
  3. Manejar errores y trazabilidad
"""

import asyncio
from typing import Any

import pyodbc

from core.trace_manager import TraceManager


class DwLoader:
    """Carga datos en staging del DW y ejecuta los SP de transformación."""

    def __init__(self, dw_connection_string: str, trace: TraceManager):
        self._conn_str = dw_connection_string
        self._trace = trace

    # ── Carga a Staging ─────────────────────────────────────────

    async def load_staging_clientes(self, records: list[dict[str, Any]], fuente: str = "CSV") -> int:
        """Inserta registros de clientes en stg_clientes."""
        sql = """
            INSERT INTO stg_clientes
                (cliente_id, nombre, apellido, email, telefono, ciudad, pais, segmento, fuente)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        rows = [
            (
                str(r.get("customer_id", r.get("CustomerID", ""))),
                r.get("first_name", r.get("FirstName", "")),
                r.get("last_name", r.get("LastName", "")),
                r.get("email", r.get("Email", "")),
                r.get("phone", r.get("Phone", "")),
                r.get("city", r.get("City", "")),
                r.get("country", r.get("Country", "")),
                r.get("segmento", "General"),
                fuente,
            )
            for r in records
        ]
        return await asyncio.to_thread(self._execute_many, sql, rows, "stg_clientes")

    async def load_staging_productos(self, records: list[dict[str, Any]], fuente: str = "CSV") -> int:
        """Inserta registros de productos en stg_productos."""
        sql = """
            INSERT INTO stg_productos
                (producto_id, nombre, categoria, precio, stock, fuente)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        rows = [
            (
                str(r.get("product_id", r.get("ProductID", ""))),
                r.get("product_name", r.get("ProductName", "")),
                r.get("category", r.get("Category", "")),
                str(r.get("price", r.get("Price", "0"))),
                str(r.get("stock", r.get("Stock", "0"))),
                fuente,
            )
            for r in records
        ]
        return await asyncio.to_thread(self._execute_many, sql, rows, "stg_productos")

    async def load_staging_ventas(self, records: list[dict[str, Any]], fuente: str = "CSV") -> int:
        """Inserta registros de ventas en stg_ventas."""
        sql = """
            INSERT INTO stg_ventas
                (venta_id, cliente_id, fecha_venta, estado, fuente)
            VALUES (?, ?, ?, ?, ?)
        """
        rows = [
            (
                str(r.get("order_id", r.get("OrderID", ""))),
                str(r.get("customer_id", r.get("CustomerID", ""))),
                r.get("order_date", r.get("OrderDate", "")),
                r.get("status", r.get("Status", "")),
                fuente,
            )
            for r in records
        ]
        return await asyncio.to_thread(self._execute_many, sql, rows, "stg_ventas")

    async def load_staging_detalle(self, records: list[dict[str, Any]], fuente: str = "CSV") -> int:
        """Inserta registros de detalle de ventas en stg_detalle_ventas."""
        sql = """
            INSERT INTO stg_detalle_ventas
                (venta_id, producto_id, cantidad, precio_unitario, total_linea, fuente)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        rows = [
            (
                str(r.get("order_id", r.get("OrderID", ""))),
                str(r.get("product_id", r.get("ProductID", ""))),
                str(r.get("quantity", r.get("Quantity", "0"))),
                str(r.get("precio_unitario", r.get("Price", "0"))),
                str(r.get("total_price", r.get("TotalPrice", "0"))),
                fuente,
            )
            for r in records
        ]
        return await asyncio.to_thread(self._execute_many, sql, rows, "stg_detalle_ventas")

    async def load_staging_api_comentarios(self, records: list[dict[str, Any]]) -> int:
        """Inserta registros de comentarios API en stg_api_comentarios."""
        sql = """
            INSERT INTO stg_api_comentarios
                (post_id, comentario_id, nombre, email, cuerpo, fuente)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        rows = [
            (
                str(r.get("post_id", r.get("postId", ""))),
                str(r.get("id", "")),
                r.get("name", ""),
                r.get("email", ""),
                r.get("body", ""),
                "API",
            )
            for r in records
        ]
        return await asyncio.to_thread(self._execute_many, sql, rows, "stg_api_comentarios")

    # ── Ejecución ETL (Staging → Dimensiones → Hechos) ─────────

    async def execute_etl_to_dw(self) -> bool:
        """Ejecuta el stored procedure que orquesta la carga al DW."""
        self._trace.start_timer("dw_etl_load")
        self._trace.info("── FASE 4: CARGA AL DATA WAREHOUSE ──")

        success = await asyncio.to_thread(self._run_etl_sp)

        self._trace.stop_timer("dw_etl_load")
        return success

    # ── Internos ────────────────────────────────────────────────

    def _execute_many(self, sql: str, rows: list[tuple], table_name: str) -> int:
        """Ejecuta un INSERT masivo con parámetros."""
        if not rows:
            return 0

        count = 0
        try:
            conn = pyodbc.connect(self._conn_str, timeout=30)
            cursor = conn.cursor()
            for row in rows:
                try:
                    cursor.execute(sql, row)
                    count += 1
                except pyodbc.Error as e:
                    self._trace.warning(f"Fila rechazada en {table_name}: {e}")
            conn.commit()
            cursor.close()
            conn.close()
            self._trace.info(f"  → {count}/{len(rows)} filas insertadas en {table_name}")
            self._trace.increment(f"stg_{table_name}_loaded", count)
        except pyodbc.Error as e:
            self._trace.error(f"Error conectando al DW para {table_name}", e)
        return count

    def _run_etl_sp(self) -> bool:
        """Ejecuta sp_EjecutarETLCompleto en el Data Warehouse."""
        try:
            conn = pyodbc.connect(self._conn_str, timeout=120)
            cursor = conn.cursor()

            self._trace.info("  Ejecutando sp_EjecutarETLCompleto...")
            cursor.execute("EXEC sp_EjecutarETLCompleto")
            conn.commit()

            cursor.close()
            conn.close()
            self._trace.info("  ✅ Carga al Data Warehouse completada")
            return True
        except pyodbc.Error as e:
            self._trace.error("Error ejecutando ETL al DW", e)
            return False
