"""
Dashboard de Ventas — Módulo de Visualización.

Conecta al Data Warehouse (VentasAnalisisDB) y presenta:
  • KPIs principales (total ventas, clientes, ticket promedio)
  • Ventas por mes
  • Ventas por categoría
  • Top 10 clientes
  • Ventas por trimestre
  • Top 10 productos más vendidos

Ejecución:
    streamlit run dashboard/app.py
"""

import sys
from pathlib import Path

import streamlit as st
import pandas as pd
import pyodbc

# ── Agregar raíz del proyecto al path para leer config ──────────────
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import Settings


# ─────────────────────────────────────────────────────────────────────
# Conexión al Data Warehouse
# ─────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_connection():
    """Crea una conexión reutilizable al Data Warehouse."""
    settings = Settings()
    return pyodbc.connect(settings.dw_connection_string, timeout=30)


def run_query(sql: str) -> pd.DataFrame:
    """Ejecuta una consulta contra VentasAnalisisDB y retorna un DataFrame."""
    conn = get_connection()
    return pd.read_sql(sql, conn)


# ─────────────────────────────────────────────────────────────────────
# Configuración de la página
# ─────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard de Ventas",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Dashboard de Ventas — Data Warehouse")
st.markdown("Visualización analítica conectada a **VentasAnalisisDB** (modelo estrella).")
st.divider()

# ─────────────────────────────────────────────────────────────────────
# KPIs Principales
# ─────────────────────────────────────────────────────────────────────
try:
    kpi_df = run_query("""
        SELECT
            COUNT(DISTINCT f.idHecho)               AS TotalVentas,
            COUNT(DISTINCT f.idCliente)              AS TotalClientes,
            COUNT(DISTINCT f.idProducto)             AS TotalProductos,
            ISNULL(SUM(f.TotalVenta), 0)             AS IngresoTotal,
            ISNULL(AVG(f.TotalVenta), 0)             AS TicketPromedio,
            ISNULL(SUM(f.Cantidad), 0)               AS UnidadesVendidas
        FROM FactVentas f
    """)

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("🛒 Total Ventas", f"{kpi_df['TotalVentas'].iloc[0]:,}")
    k2.metric("👥 Clientes", f"{kpi_df['TotalClientes'].iloc[0]:,}")
    k3.metric("📦 Productos", f"{kpi_df['TotalProductos'].iloc[0]:,}")
    k4.metric("💰 Ingreso Total", f"${kpi_df['IngresoTotal'].iloc[0]:,.2f}")
    k5.metric("🎫 Ticket Promedio", f"${kpi_df['TicketPromedio'].iloc[0]:,.2f}")
    k6.metric("📈 Unidades", f"{kpi_df['UnidadesVendidas'].iloc[0]:,}")

    st.divider()

    # ── Fila 1: Ventas por Mes  |  Ventas por Categoría ────────────
    col_left, col_right = st.columns(2)

    # ── Ventas por Mes ──────────────────────────────────────────────
    with col_left:
        st.subheader("📅 Ventas por Mes")
        ventas_mes = run_query("""
            SELECT
                t.Anio,
                t.Mes,
                t.NombreMes,
                COUNT(f.idHecho)      AS CantidadVentas,
                SUM(f.TotalVenta)     AS TotalIngresos
            FROM FactVentas f
            INNER JOIN DimTiempo t ON f.idTiempo = t.idTiempo
            GROUP BY t.Anio, t.Mes, t.NombreMes
            ORDER BY t.Anio, t.Mes
        """)
        if not ventas_mes.empty:
            ventas_mes["Periodo"] = ventas_mes["NombreMes"] + " " + ventas_mes["Anio"].astype(str)
            st.bar_chart(ventas_mes.set_index("Periodo")["TotalIngresos"])
            with st.expander("Ver datos"):
                st.dataframe(ventas_mes[["Periodo", "CantidadVentas", "TotalIngresos"]], use_container_width=True)
        else:
            st.info("Sin datos de ventas por mes.")

    # ── Ventas por Categoría ────────────────────────────────────────
    with col_right:
        st.subheader("🏷️ Ventas por Categoría")
        ventas_cat = run_query("""
            SELECT
                c.NombreCategoria       AS Categoria,
                COUNT(f.idHecho)        AS CantidadVentas,
                SUM(f.TotalVenta)       AS TotalIngresos,
                AVG(f.TotalVenta)       AS PromedioVenta
            FROM FactVentas f
            INNER JOIN DimCategoria c ON f.idCategoria = c.idCategoria
            GROUP BY c.NombreCategoria
            ORDER BY TotalIngresos DESC
        """)
        if not ventas_cat.empty:
            st.bar_chart(ventas_cat.set_index("Categoria")["TotalIngresos"])
            with st.expander("Ver datos"):
                st.dataframe(ventas_cat, use_container_width=True)
        else:
            st.info("Sin datos de ventas por categoría.")

    st.divider()

    # ── Fila 2: Top Clientes  |  Ventas por Trimestre ──────────────
    col_left2, col_right2 = st.columns(2)

    # ── Top 10 Clientes ────────────────────────────────────────────
    with col_left2:
        st.subheader("🏆 Top 10 Clientes por Ingreso")
        top_clientes = run_query("""
            SELECT TOP 10
                cl.NombreCompleto       AS Cliente,
                cl.Pais,
                COUNT(f.idHecho)        AS CantidadCompras,
                SUM(f.TotalVenta)       AS TotalGastado
            FROM FactVentas f
            INNER JOIN DimCliente cl ON f.idCliente = cl.idCliente
            GROUP BY cl.NombreCompleto, cl.Pais
            ORDER BY TotalGastado DESC
        """)
        if not top_clientes.empty:
            st.bar_chart(top_clientes.set_index("Cliente")["TotalGastado"])
            with st.expander("Ver datos"):
                st.dataframe(top_clientes, use_container_width=True)
        else:
            st.info("Sin datos de clientes.")

    # ── Ventas por Trimestre ───────────────────────────────────────
    with col_right2:
        st.subheader("📊 Ventas por Trimestre")
        ventas_trim = run_query("""
            SELECT
                t.Anio,
                t.Trimestre,
                COUNT(f.idHecho)        AS CantidadVentas,
                SUM(f.TotalVenta)       AS TotalIngresos
            FROM FactVentas f
            INNER JOIN DimTiempo t ON f.idTiempo = t.idTiempo
            GROUP BY t.Anio, t.Trimestre
            ORDER BY t.Anio, t.Trimestre
        """)
        if not ventas_trim.empty:
            ventas_trim["Periodo"] = "Q" + ventas_trim["Trimestre"].astype(str) + " " + ventas_trim["Anio"].astype(str)
            st.bar_chart(ventas_trim.set_index("Periodo")["TotalIngresos"])
            with st.expander("Ver datos"):
                st.dataframe(ventas_trim[["Periodo", "CantidadVentas", "TotalIngresos"]], use_container_width=True)
        else:
            st.info("Sin datos de ventas por trimestre.")

    st.divider()

    # ── Fila 3: Top Productos  |  Ventas Fin de Semana vs Laboral ─
    col_left3, col_right3 = st.columns(2)

    # ── Top 10 Productos ───────────────────────────────────────────
    with col_left3:
        st.subheader("📦 Top 10 Productos Más Vendidos")
        top_prod = run_query("""
            SELECT TOP 10
                p.NombreProducto        AS Producto,
                p.Categoria,
                SUM(f.Cantidad)         AS UnidadesVendidas,
                SUM(f.TotalVenta)       AS TotalIngresos
            FROM FactVentas f
            INNER JOIN DimProducto p ON f.idProducto = p.idProducto
            GROUP BY p.NombreProducto, p.Categoria
            ORDER BY UnidadesVendidas DESC
        """)
        if not top_prod.empty:
            st.bar_chart(top_prod.set_index("Producto")["UnidadesVendidas"])
            with st.expander("Ver datos"):
                st.dataframe(top_prod, use_container_width=True)
        else:
            st.info("Sin datos de productos.")

    # ── Ventas: Fin de semana vs Laboral ───────────────────────────
    with col_right3:
        st.subheader("📆 Ventas: Laboral vs Fin de Semana")
        ventas_fds = run_query("""
            SELECT
                CASE WHEN t.EsFinDeSemana = 1 THEN 'Fin de Semana' ELSE 'Día Laboral' END AS TipoDia,
                COUNT(f.idHecho)        AS CantidadVentas,
                SUM(f.TotalVenta)       AS TotalIngresos
            FROM FactVentas f
            INNER JOIN DimTiempo t ON f.idTiempo = t.idTiempo
            GROUP BY t.EsFinDeSemana
        """)
        if not ventas_fds.empty:
            st.bar_chart(ventas_fds.set_index("TipoDia")["TotalIngresos"])
            with st.expander("Ver datos"):
                st.dataframe(ventas_fds, use_container_width=True)
        else:
            st.info("Sin datos de ventas.")

    # ── Footer ─────────────────────────────────────────────────────
    st.divider()
    st.caption("Dashboard conectado a VentasAnalisisDB — Modelo Estrella (Star Schema)")

except pyodbc.Error as e:
    st.error(f"⚠️ Error de conexión al Data Warehouse: {e}")
    st.info(
        "Asegúrate de:\n"
        "1. Tener SQL Server LocalDB instalado y en ejecución\n"
        "2. Haber creado la base VentasAnalisisDB ejecutando `database/VentasAnalisis.sql`\n"
        "3. Haber ejecutado el proceso ETL (`python main.py`) para cargar datos"
    )
except Exception as e:
    st.error(f"Error inesperado: {e}")
