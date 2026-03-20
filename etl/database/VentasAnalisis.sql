

-- =====================================================================
-- VENTASANALISISDB — BASE DE DATOS ANALÍTICA (DATA WAREHOUSE)
-- Motor   : SQL Server — (localdb)\MSSQLLocalDB
-- Archivo : database/VentasAnalisis.sql
-- Diseño  : Modelo Estrella (Star Schema)
-- =====================================================================
--
--  ARQUITECTURA DEL MODELO ESTRELLA:
--
--          ┌──────────────┐
--          │  DimCliente  │
--          └──────┬───────┘
--                 │
--  ┌────────────┐ │ ┌──────────────┐
--  │ DimTiempo  │─┼─│ DimCategoria │
--  └────────────┘ │ └──────────────┘
--                 │
--          ┌──────┴───────┐
--          │  FactVentas  │  ← Tabla de Hechos (centro)
--          └──────┬───────┘
--                 │
--          ┌──────┴───────┐
--          │ DimProducto  │
--          └──────────────┘
--
-- =====================================================================
-- Atributos de calidad:
--   • Rendimiento    → Índices en FK de hechos y columnas de agregación
--   • Escalabilidad  → Agregar dimensiones sin modificar hechos
--   • Seguridad      → Sin credenciales hardcodeadas
--   • Mantenibilidad → Separación OLTP / Staging / DW, nombres claros
--   • Desnormalización→ Dimensiones planas para consultas rápidas
-- =====================================================================

-- ─────────────────────────────────────────────────────────────────────
-- 0. CREACIÓN DE LA BASE DE DATOS ANALÍTICA
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'VentasAnalisisDB')
BEGIN
    CREATE DATABASE VentasAnalisisDB;
END
GO

USE VentasAnalisisDB;
GO

-- =====================================================================
--                    ZONA DE STAGING (INTERMEDIA)
-- =====================================================================
-- Tablas sin restricciones complejas para carga rápida desde ETL.
-- Los datos llegan aquí antes de ser transformados y cargados al DW.
-- =====================================================================

-- ─────────────────────────────────────────────────────────────────────
-- STG-1. stg_clientes — datos crudos de clientes
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'stg_clientes')
BEGIN
    CREATE TABLE stg_clientes (
        id_carga        INT IDENTITY(1,1) NOT NULL,
        cliente_id      NVARCHAR(50)      NULL,
        nombre          NVARCHAR(100)     NULL,
        apellido        NVARCHAR(100)     NULL,
        email           NVARCHAR(200)     NULL,
        telefono        NVARCHAR(100)     NULL,
        ciudad          NVARCHAR(100)     NULL,
        pais            NVARCHAR(100)     NULL,
        segmento        NVARCHAR(50)      NULL,
        fuente          NVARCHAR(50)      NULL,   -- 'CSV', 'OLTP', 'API'
        fecha_carga     DATETIME2         NOT NULL DEFAULT GETDATE(),
        procesado       BIT               NOT NULL DEFAULT 0,
        CONSTRAINT PK_stg_clientes PRIMARY KEY (id_carga)
    );
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- STG-2. stg_productos — datos crudos de productos
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'stg_productos')
BEGIN
    CREATE TABLE stg_productos (
        id_carga        INT IDENTITY(1,1) NOT NULL,
        producto_id     NVARCHAR(50)      NULL,
        nombre          NVARCHAR(200)     NULL,
        categoria       NVARCHAR(100)     NULL,
        precio          NVARCHAR(50)      NULL,
        stock           NVARCHAR(50)      NULL,
        fuente          NVARCHAR(50)      NULL,
        fecha_carga     DATETIME2         NOT NULL DEFAULT GETDATE(),
        procesado       BIT               NOT NULL DEFAULT 0,
        CONSTRAINT PK_stg_productos PRIMARY KEY (id_carga)
    );
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- STG-3. stg_ventas — datos crudos de ventas/órdenes
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'stg_ventas')
BEGIN
    CREATE TABLE stg_ventas (
        id_carga        INT IDENTITY(1,1) NOT NULL,
        venta_id        NVARCHAR(50)      NULL,
        cliente_id      NVARCHAR(50)      NULL,
        fecha_venta     NVARCHAR(50)      NULL,
        estado          NVARCHAR(50)      NULL,
        fuente          NVARCHAR(50)      NULL,
        fecha_carga     DATETIME2         NOT NULL DEFAULT GETDATE(),
        procesado       BIT               NOT NULL DEFAULT 0,
        CONSTRAINT PK_stg_ventas PRIMARY KEY (id_carga)
    );
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- STG-4. stg_detalle_ventas — datos crudos de líneas de venta
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'stg_detalle_ventas')
BEGIN
    CREATE TABLE stg_detalle_ventas (
        id_carga        INT IDENTITY(1,1) NOT NULL,
        venta_id        NVARCHAR(50)      NULL,
        producto_id     NVARCHAR(50)      NULL,
        cantidad        NVARCHAR(50)      NULL,
        precio_unitario NVARCHAR(50)      NULL,
        total_linea     NVARCHAR(50)      NULL,
        fuente          NVARCHAR(50)      NULL,
        fecha_carga     DATETIME2         NOT NULL DEFAULT GETDATE(),
        procesado       BIT               NOT NULL DEFAULT 0,
        CONSTRAINT PK_stg_detalle_ventas PRIMARY KEY (id_carga)
    );
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- STG-5. stg_api_comentarios — datos crudos desde API REST
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'stg_api_comentarios')
BEGIN
    CREATE TABLE stg_api_comentarios (
        id_carga        INT IDENTITY(1,1) NOT NULL,
        post_id         NVARCHAR(50)      NULL,
        comentario_id   NVARCHAR(50)      NULL,
        nombre          NVARCHAR(500)     NULL,
        email           NVARCHAR(200)     NULL,
        cuerpo          NVARCHAR(MAX)     NULL,
        fuente          NVARCHAR(50)      NULL DEFAULT N'API',
        fecha_carga     DATETIME2         NOT NULL DEFAULT GETDATE(),
        procesado       BIT               NOT NULL DEFAULT 0,
        CONSTRAINT PK_stg_api_comentarios PRIMARY KEY (id_carga)
    );
END
GO

-- =====================================================================
--              TABLAS DIMENSIONALES (MODELO ESTRELLA)
-- =====================================================================
-- Características:
--   • Claves sustitutas (IDENTITY) independientes del sistema OLTP
--   • Desnormalizadas para consultas rápidas
--   • Preparadas para dashboards y reportes BI
-- =====================================================================

-- ─────────────────────────────────────────────────────────────────────
-- DIM-1. DimTiempo — Dimensión Tiempo (OBLIGATORIA para BI)
-- ─────────────────────────────────────────────────────────────────────
-- Permite analizar: ventas por mes, por trimestre, por año,
-- comparaciones históricas, tendencias estacionales.
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'DimTiempo')
BEGIN
    CREATE TABLE DimTiempo (
        idTiempo        INT IDENTITY(1,1) NOT NULL,
        Fecha           DATE              NOT NULL,
        Anio            INT               NOT NULL,
        Mes             INT               NOT NULL,
        NombreMes       NVARCHAR(20)      NOT NULL,
        Trimestre       INT               NOT NULL,
        Dia             INT               NOT NULL,
        DiaSemana       INT               NOT NULL,
        NombreDia       NVARCHAR(20)      NOT NULL,
        Semana          INT               NOT NULL,
        EsFinDeSemana   BIT               NOT NULL DEFAULT 0,
        CONSTRAINT PK_DimTiempo PRIMARY KEY (idTiempo),
        CONSTRAINT UQ_DimTiempo_Fecha UNIQUE (Fecha)
    );

    -- Índices para consultas analíticas frecuentes
    CREATE NONCLUSTERED INDEX IX_DimTiempo_Anio_Mes
        ON DimTiempo (Anio, Mes);

    CREATE NONCLUSTERED INDEX IX_DimTiempo_Trimestre
        ON DimTiempo (Anio, Trimestre);
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- Poblar DimTiempo automáticamente (2023-01-01 a 2026-12-31)
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM DimTiempo)
BEGIN
    DECLARE @fecha DATE = '2023-01-01';
    DECLARE @fin   DATE = '2026-12-31';

    WHILE @fecha <= @fin
    BEGIN
        INSERT INTO DimTiempo (
            Fecha, Anio, Mes, NombreMes, Trimestre,
            Dia, DiaSemana, NombreDia, Semana, EsFinDeSemana
        )
        VALUES (
            @fecha,
            YEAR(@fecha),
            MONTH(@fecha),
            DATENAME(MONTH, @fecha),
            DATEPART(QUARTER, @fecha),
            DAY(@fecha),
            DATEPART(WEEKDAY, @fecha),
            DATENAME(WEEKDAY, @fecha),
            DATEPART(WEEK, @fecha),
            CASE WHEN DATEPART(WEEKDAY, @fecha) IN (1, 7) THEN 1 ELSE 0 END
        );

        SET @fecha = DATEADD(DAY, 1, @fecha);
    END
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- DIM-2. DimCliente — Dimensión Cliente (desnormalizada)
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'DimCliente')
BEGIN
    CREATE TABLE DimCliente (
        idCliente       INT IDENTITY(1,1) NOT NULL,  -- Clave sustituta
        ClienteIDOrigen INT               NOT NULL,  -- Clave natural del OLTP
        NombreCompleto  NVARCHAR(200)     NOT NULL,
        Email           NVARCHAR(200)     NULL,
        Telefono        NVARCHAR(100)     NULL,
        Ciudad          NVARCHAR(100)     NULL,
        Pais            NVARCHAR(100)     NULL,
        Segmento        NVARCHAR(50)      NOT NULL DEFAULT N'General',
        FechaRegistro   DATE              NULL,
        Activo          BIT               NOT NULL DEFAULT 1,
        FechaCargaDW    DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_DimCliente PRIMARY KEY (idCliente),
        CONSTRAINT UQ_DimCliente_Origen UNIQUE (ClienteIDOrigen)
    );

    CREATE NONCLUSTERED INDEX IX_DimCliente_Pais
        ON DimCliente (Pais);

    CREATE NONCLUSTERED INDEX IX_DimCliente_Segmento
        ON DimCliente (Segmento);
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- DIM-3. DimCategoria — Dimensión Categoría
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'DimCategoria')
BEGIN
    CREATE TABLE DimCategoria (
        idCategoria       INT IDENTITY(1,1) NOT NULL,  -- Clave sustituta
        CategoriaIDOrigen INT               NULL,      -- Clave natural del OLTP
        NombreCategoria   NVARCHAR(100)     NOT NULL,
        Descripcion       NVARCHAR(255)     NULL,
        FechaCargaDW      DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_DimCategoria PRIMARY KEY (idCategoria),
        CONSTRAINT UQ_DimCategoria_Nombre UNIQUE (NombreCategoria)
    );
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- DIM-4. DimProducto — Dimensión Producto (desnormalizada con categoría)
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'DimProducto')
BEGIN
    CREATE TABLE DimProducto (
        idProducto        INT IDENTITY(1,1) NOT NULL,  -- Clave sustituta
        ProductoIDOrigen  INT               NOT NULL,  -- Clave natural del OLTP
        NombreProducto    NVARCHAR(200)     NOT NULL,
        Categoria         NVARCHAR(100)     NOT NULL,  -- Desnormalizado desde DimCategoria
        Precio            DECIMAL(10,2)     NOT NULL,
        Estado            NVARCHAR(20)      NOT NULL DEFAULT N'Activo',
        FechaCargaDW      DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_DimProducto PRIMARY KEY (idProducto),
        CONSTRAINT UQ_DimProducto_Origen UNIQUE (ProductoIDOrigen)
    );

    CREATE NONCLUSTERED INDEX IX_DimProducto_Categoria
        ON DimProducto (Categoria);
END
GO

-- =====================================================================
--               TABLA DE HECHOS (CENTRO DEL MODELO)
-- =====================================================================
-- FactVentas contiene las métricas cuantificables del negocio.
-- Cada fila = una línea de detalle de venta.
-- Las FK apuntan a las dimensiones (estrella).
-- =====================================================================

-- ─────────────────────────────────────────────────────────────────────
-- FACT. FactVentas — Tabla de Hechos principal
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'FactVentas')
BEGIN
    CREATE TABLE FactVentas (
        idHecho           INT IDENTITY(1,1) NOT NULL,  -- PK autoincremental
        -- Claves foráneas a dimensiones (modelo estrella)
        idCliente         INT               NOT NULL,
        idProducto        INT               NOT NULL,
        idTiempo          INT               NOT NULL,
        idCategoria       INT               NOT NULL,
        -- Métricas / Medidas
        Cantidad          INT               NOT NULL,
        PrecioUnitario    DECIMAL(10,2)     NOT NULL,
        TotalVenta        DECIMAL(12,2)     NOT NULL,
        -- Atributos degenerados (de la venta original)
        VentaIDOrigen     INT               NOT NULL,
        EstadoVenta       NVARCHAR(50)      NOT NULL,
        -- Auditoría
        FechaCargaDW      DATETIME2         NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_FactVentas PRIMARY KEY (idHecho),
        CONSTRAINT FK_Fact_DimCliente
            FOREIGN KEY (idCliente)   REFERENCES DimCliente(idCliente),
        CONSTRAINT FK_Fact_DimProducto
            FOREIGN KEY (idProducto)  REFERENCES DimProducto(idProducto),
        CONSTRAINT FK_Fact_DimTiempo
            FOREIGN KEY (idTiempo)    REFERENCES DimTiempo(idTiempo),
        CONSTRAINT FK_Fact_DimCategoria
            FOREIGN KEY (idCategoria) REFERENCES DimCategoria(idCategoria)
    );

    -- ─── Índices optimizados para consultas analíticas ───
    -- Índices en cada FK (rendimiento en JOINs con dimensiones)
    CREATE NONCLUSTERED INDEX IX_Fact_Cliente
        ON FactVentas (idCliente);

    CREATE NONCLUSTERED INDEX IX_Fact_Producto
        ON FactVentas (idProducto);

    CREATE NONCLUSTERED INDEX IX_Fact_Tiempo
        ON FactVentas (idTiempo);

    CREATE NONCLUSTERED INDEX IX_Fact_Categoria
        ON FactVentas (idCategoria);

    -- Índice compuesto para consultas de ventas por tiempo y categoría
    CREATE NONCLUSTERED INDEX IX_Fact_Tiempo_Categoria
        ON FactVentas (idTiempo, idCategoria)
        INCLUDE (Cantidad, TotalVenta);

    -- Índice para agregaciones por cliente y tiempo
    CREATE NONCLUSTERED INDEX IX_Fact_Cliente_Tiempo
        ON FactVentas (idCliente, idTiempo)
        INCLUDE (TotalVenta);
END
GO

-- =====================================================================
--            PROCEDIMIENTOS ETL: CARGA STAGING → DW
-- =====================================================================

-- ─────────────────────────────────────────────────────────────────────
-- ETL-1. sp_CargarDimCategoria
-- Carga la dimensión categoría desde staging de productos
-- ─────────────────────────────────────────────────────────────────────
IF OBJECT_ID('sp_CargarDimCategoria', 'P') IS NOT NULL
    DROP PROCEDURE sp_CargarDimCategoria;
GO

CREATE PROCEDURE sp_CargarDimCategoria
AS
BEGIN
    SET NOCOUNT ON;

    -- Insertar categorías nuevas que no existan en la dimensión
    INSERT INTO DimCategoria (NombreCategoria, Descripcion)
    SELECT DISTINCT
        LTRIM(RTRIM(s.categoria)),
        N'Categoría importada desde staging'
    FROM stg_productos s
    WHERE s.procesado = 0
      AND s.categoria IS NOT NULL
      AND LTRIM(RTRIM(s.categoria)) <> ''
      AND NOT EXISTS (
          SELECT 1 FROM DimCategoria d
          WHERE d.NombreCategoria = LTRIM(RTRIM(s.categoria))
      );

    PRINT '✅ DimCategoria cargada.';
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- ETL-2. sp_CargarDimCliente
-- Carga la dimensión cliente desde staging
-- ─────────────────────────────────────────────────────────────────────
IF OBJECT_ID('sp_CargarDimCliente', 'P') IS NOT NULL
    DROP PROCEDURE sp_CargarDimCliente;
GO

CREATE PROCEDURE sp_CargarDimCliente
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO DimCliente (
        ClienteIDOrigen, NombreCompleto, Email, Telefono,
        Ciudad, Pais, Segmento, FechaRegistro
    )
    SELECT DISTINCT
        CAST(s.cliente_id AS INT),
        LTRIM(RTRIM(ISNULL(s.nombre, ''))) + ' ' + LTRIM(RTRIM(ISNULL(s.apellido, ''))),
        s.email,
        s.telefono,
        s.ciudad,
        s.pais,
        ISNULL(s.segmento, N'General'),
        GETDATE()
    FROM stg_clientes s
    WHERE s.procesado = 0
      AND s.cliente_id IS NOT NULL
      AND TRY_CAST(s.cliente_id AS INT) IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM DimCliente d
          WHERE d.ClienteIDOrigen = CAST(s.cliente_id AS INT)
      );

    -- Marcar como procesados
    UPDATE stg_clientes SET procesado = 1 WHERE procesado = 0;

    PRINT '✅ DimCliente cargada.';
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- ETL-3. sp_CargarDimProducto
-- Carga la dimensión producto desde staging (desnormalizada con categoría)
-- ─────────────────────────────────────────────────────────────────────
IF OBJECT_ID('sp_CargarDimProducto', 'P') IS NOT NULL
    DROP PROCEDURE sp_CargarDimProducto;
GO

CREATE PROCEDURE sp_CargarDimProducto
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO DimProducto (
        ProductoIDOrigen, NombreProducto, Categoria, Precio, Estado
    )
    SELECT DISTINCT
        CAST(s.producto_id AS INT),
        LTRIM(RTRIM(s.nombre)),
        LTRIM(RTRIM(ISNULL(s.categoria, N'Sin Categoría'))),
        CAST(s.precio AS DECIMAL(10,2)),
        N'Activo'
    FROM stg_productos s
    WHERE s.procesado = 0
      AND s.producto_id IS NOT NULL
      AND TRY_CAST(s.producto_id AS INT) IS NOT NULL
      AND TRY_CAST(s.precio AS DECIMAL(10,2)) IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM DimProducto d
          WHERE d.ProductoIDOrigen = CAST(s.producto_id AS INT)
      );

    -- Marcar como procesados
    UPDATE stg_productos SET procesado = 1 WHERE procesado = 0;

    PRINT '✅ DimProducto cargada.';
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- ETL-4. sp_CargarFactVentas
-- Carga la tabla de hechos cruzando staging con dimensiones.
-- Las dimensiones DEBEN estar cargadas previamente.
-- ─────────────────────────────────────────────────────────────────────
IF OBJECT_ID('sp_CargarFactVentas', 'P') IS NOT NULL
    DROP PROCEDURE sp_CargarFactVentas;
GO

CREATE PROCEDURE sp_CargarFactVentas
AS
BEGIN
    SET NOCOUNT ON;

    -- Insertar hechos: cada línea de detalle = un hecho
    INSERT INTO FactVentas (
        idCliente, idProducto, idTiempo, idCategoria,
        Cantidad, PrecioUnitario, TotalVenta,
        VentaIDOrigen, EstadoVenta
    )
    SELECT
        dc.idCliente,
        dp.idProducto,
        dt.idTiempo,
        dcat.idCategoria,
        CAST(sd.cantidad AS INT),
        CAST(sd.precio_unitario AS DECIMAL(10,2)),
        CAST(sd.total_linea AS DECIMAL(12,2)),
        CAST(sv.venta_id AS INT),
        sv.estado
    FROM stg_detalle_ventas sd
    -- Join con staging ventas para obtener cliente y fecha
    INNER JOIN stg_ventas sv
        ON sd.venta_id = sv.venta_id
           AND sv.procesado = 0
    -- Join con dimensión cliente
    INNER JOIN DimCliente dc
        ON dc.ClienteIDOrigen = CAST(sv.cliente_id AS INT)
    -- Join con dimensión producto
    INNER JOIN DimProducto dp
        ON dp.ProductoIDOrigen = CAST(sd.producto_id AS INT)
    -- Join con dimensión tiempo via fecha de venta
    INNER JOIN DimTiempo dt
        ON dt.Fecha = CAST(sv.fecha_venta AS DATE)
    -- Join con dimensión categoría via producto
    INNER JOIN DimCategoria dcat
        ON dcat.NombreCategoria = dp.Categoria
    WHERE sd.procesado = 0
      AND TRY_CAST(sd.cantidad AS INT) IS NOT NULL
      AND TRY_CAST(sd.total_linea AS DECIMAL(12,2)) IS NOT NULL
      -- Evitar duplicados
      AND NOT EXISTS (
          SELECT 1 FROM FactVentas f
          WHERE f.VentaIDOrigen = CAST(sv.venta_id AS INT)
            AND f.idProducto    = dp.idProducto
      );

    -- Marcar staging como procesado
    UPDATE stg_ventas         SET procesado = 1 WHERE procesado = 0;
    UPDATE stg_detalle_ventas SET procesado = 1 WHERE procesado = 0;

    PRINT '✅ FactVentas cargada.';
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- ETL-5. sp_EjecutarETLCompleto
-- Orquesta todo el proceso ETL en orden correcto:
--   1. Dimensiones (primero categorías, luego clientes y productos)
--   2. Tabla de hechos (al final, cuando las dimensiones existen)
-- ─────────────────────────────────────────────────────────────────────
IF OBJECT_ID('sp_EjecutarETLCompleto', 'P') IS NOT NULL
    DROP PROCEDURE sp_EjecutarETLCompleto;
GO

CREATE PROCEDURE sp_EjecutarETLCompleto
AS
BEGIN
    SET NOCOUNT ON;

    PRINT '══════════════════════════════════════════════════════════';
    PRINT '  🔄 INICIO DEL PROCESO ETL → DATA WAREHOUSE';
    PRINT '══════════════════════════════════════════════════════════';

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Paso 1: Cargar dimensiones (orden importa por dependencias)
        PRINT '  [1/4] Cargando DimCategoria...';
        EXEC sp_CargarDimCategoria;

        PRINT '  [2/4] Cargando DimCliente...';
        EXEC sp_CargarDimCliente;

        PRINT '  [3/4] Cargando DimProducto...';
        EXEC sp_CargarDimProducto;

        -- Paso 2: Cargar tabla de hechos
        PRINT '  [4/4] Cargando FactVentas...';
        EXEC sp_CargarFactVentas;

        COMMIT TRANSACTION;

        PRINT '══════════════════════════════════════════════════════════';
        PRINT '  ✅ ETL COMPLETADO EXITOSAMENTE';
        PRINT '══════════════════════════════════════════════════════════';
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;
        PRINT '  ❌ ERROR EN ETL: ' + ERROR_MESSAGE();
        THROW;
    END CATCH
END
GO

-- =====================================================================
--         CONSULTAS ANALÍTICAS DE EJEMPLO (DASHBOARDS / BI)
-- =====================================================================

-- ─── KPI 1: Ventas totales por mes y año ─────────────────────────────
-- SELECT
--     t.Anio, t.Mes, t.NombreMes,
--     COUNT(f.idHecho)       AS NumeroVentas,
--     SUM(f.Cantidad)        AS UnidadesTotales,
--     SUM(f.TotalVenta)      AS IngresoTotal,
--     AVG(f.TotalVenta)      AS TicketPromedio
-- FROM FactVentas f
-- INNER JOIN DimTiempo t ON f.idTiempo = t.idTiempo
-- GROUP BY t.Anio, t.Mes, t.NombreMes
-- ORDER BY t.Anio, t.Mes;

-- ─── KPI 2: Top 10 clientes por ingreso ──────────────────────────────
-- SELECT TOP 10
--     c.NombreCompleto, c.Pais, c.Segmento,
--     COUNT(f.idHecho)   AS NumeroCompras,
--     SUM(f.TotalVenta)  AS TotalGastado
-- FROM FactVentas f
-- INNER JOIN DimCliente c ON f.idCliente = c.idCliente
-- GROUP BY c.NombreCompleto, c.Pais, c.Segmento
-- ORDER BY TotalGastado DESC;

-- ─── KPI 3: Ventas por categoría ─────────────────────────────────────
-- SELECT
--     cat.NombreCategoria,
--     COUNT(f.idHecho)       AS NumeroVentas,
--     SUM(f.Cantidad)        AS UnidadesVendidas,
--     SUM(f.TotalVenta)      AS IngresoTotal,
--     AVG(f.PrecioUnitario)  AS PrecioPromedio
-- FROM FactVentas f
-- INNER JOIN DimCategoria cat ON f.idCategoria = cat.idCategoria
-- GROUP BY cat.NombreCategoria
-- ORDER BY IngresoTotal DESC;

-- ─── KPI 4: Ventas por trimestre ─────────────────────────────────────
-- SELECT
--     t.Anio,
--     t.Trimestre,
--     SUM(f.TotalVenta)  AS IngresoTrimestral,
--     SUM(f.Cantidad)    AS UnidadesTrimestral
-- FROM FactVentas f
-- INNER JOIN DimTiempo t ON f.idTiempo = t.idTiempo
-- GROUP BY t.Anio, t.Trimestre
-- ORDER BY t.Anio, t.Trimestre;

-- ─── KPI 5: Productos más vendidos ───────────────────────────────────
-- SELECT TOP 10
--     p.NombreProducto, p.Categoria, p.Precio,
--     SUM(f.Cantidad)    AS UnidadesVendidas,
--     SUM(f.TotalVenta)  AS IngresoTotal
-- FROM FactVentas f
-- INNER JOIN DimProducto p ON f.idProducto = p.idProducto
-- GROUP BY p.NombreProducto, p.Categoria, p.Precio
-- ORDER BY UnidadesVendidas DESC;

PRINT '✅ VentasAnalisisDB (Data Warehouse) creada correctamente.';
GO


