

-- =====================================================================
-- VENTASDB — BASE DE DATOS OPERACIONAL (OLTP)
-- Motor   : SQL Server — (localdb)\MSSQLLocalDB
-- Archivo : database/Ventas.sql
-- Diseño  : Modelo relacional normalizado (3FN)
-- =====================================================================
-- Atributos de calidad:
--   • Rendimiento   → Índices en FK y columnas de búsqueda frecuente
--   • Escalabilidad → Catálogo de categorías separado, fácil agregar entidades
--   • Seguridad     → Sin credenciales; conexión vía config externo
--   • Mantenibilidad→ Nombres claros, constraints nombrados, idempotente
-- =====================================================================

-- ─────────────────────────────────────────────────────────────────────
-- 0. CREACIÓN DE LA BASE DE DATOS
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = N'VentasDB')
BEGIN
    CREATE DATABASE VentasDB;
END
GO

USE VentasDB;
GO

-- ─────────────────────────────────────────────────────────────────────
-- 1. TABLA: Categorias  (catálogo normalizado — 3FN)
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Categorias')
BEGIN
    CREATE TABLE Categorias (
        CategoriaID   INT IDENTITY(1,1)  NOT NULL,
        Nombre        NVARCHAR(100)      NOT NULL,
        Descripcion   NVARCHAR(255)      NULL,
        FechaCreacion DATETIME2          NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_Categorias PRIMARY KEY (CategoriaID),
        CONSTRAINT UQ_Categorias_Nombre UNIQUE (Nombre)
    );
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- 2. TABLA: Clientes
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Clientes')
BEGIN
    CREATE TABLE Clientes (
        ClienteID     INT                NOT NULL,
        Nombre        NVARCHAR(100)      NOT NULL,
        Apellido      NVARCHAR(100)      NOT NULL,
        Email         NVARCHAR(200)      NULL,
        Telefono      NVARCHAR(50)       NULL,
        Ciudad        NVARCHAR(100)      NULL,
        Pais          NVARCHAR(100)      NULL,
        Segmento      NVARCHAR(50)       NULL DEFAULT N'General',
        FechaRegistro DATETIME2          NOT NULL DEFAULT GETDATE(),
        Activo        BIT                NOT NULL DEFAULT 1,
        CONSTRAINT PK_Clientes PRIMARY KEY (ClienteID)
    );

    -- Índice para búsquedas frecuentes por país / ciudad
    CREATE NONCLUSTERED INDEX IX_Clientes_Pais
        ON Clientes (Pais, Ciudad);

    CREATE NONCLUSTERED INDEX IX_Clientes_Email
        ON Clientes (Email);
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- 3. TABLA: Productos
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Productos')
BEGIN
    CREATE TABLE Productos (
        ProductoID    INT                NOT NULL,
        Nombre        NVARCHAR(200)      NOT NULL,
        CategoriaID   INT                NOT NULL,
        Precio        DECIMAL(10,2)      NOT NULL,
        Stock         INT                NOT NULL DEFAULT 0,
        Estado        NVARCHAR(20)       NOT NULL DEFAULT N'Activo',
        FechaCreacion DATETIME2          NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_Productos PRIMARY KEY (ProductoID),
        CONSTRAINT FK_Productos_Categorias
            FOREIGN KEY (CategoriaID) REFERENCES Categorias(CategoriaID),
        CONSTRAINT CK_Productos_Precio CHECK (Precio >= 0),
        CONSTRAINT CK_Productos_Stock  CHECK (Stock  >= 0)
    );

    CREATE NONCLUSTERED INDEX IX_Productos_Categoria
        ON Productos (CategoriaID);
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- 4. TABLA: Ventas  (cabecera de la venta / orden)
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'Ventas')
BEGIN
    CREATE TABLE Ventas (
        VentaID       INT                NOT NULL,
        ClienteID     INT                NOT NULL,
        FechaVenta    DATE               NOT NULL,
        Estado        NVARCHAR(50)       NOT NULL,
        FechaCarga    DATETIME2          NOT NULL DEFAULT GETDATE(),
        CONSTRAINT PK_Ventas PRIMARY KEY (VentaID),
        CONSTRAINT FK_Ventas_Clientes
            FOREIGN KEY (ClienteID) REFERENCES Clientes(ClienteID)
    );

    CREATE NONCLUSTERED INDEX IX_Ventas_Cliente
        ON Ventas (ClienteID);

    CREATE NONCLUSTERED INDEX IX_Ventas_Fecha
        ON Ventas (FechaVenta);

    CREATE NONCLUSTERED INDEX IX_Ventas_Estado
        ON Ventas (Estado);
END
GO

-- ─────────────────────────────────────────────────────────────────────
-- 5. TABLA: DetalleVentas  (líneas de cada venta)
-- ─────────────────────────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = N'DetalleVentas')
BEGIN
    CREATE TABLE DetalleVentas (
        VentaID       INT                NOT NULL,
        ProductoID    INT                NOT NULL,
        Cantidad      INT                NOT NULL,
        PrecioUnitario DECIMAL(10,2)     NOT NULL,
        TotalLinea    DECIMAL(12,2)      NOT NULL,
        CONSTRAINT PK_DetalleVentas PRIMARY KEY (VentaID, ProductoID),
        CONSTRAINT FK_DV_Ventas
            FOREIGN KEY (VentaID)    REFERENCES Ventas(VentaID),
        CONSTRAINT FK_DV_Productos
            FOREIGN KEY (ProductoID) REFERENCES Productos(ProductoID),
        CONSTRAINT CK_DV_Cantidad CHECK (Cantidad > 0),
        CONSTRAINT CK_DV_Total   CHECK (TotalLinea >= 0)
    );

    CREATE NONCLUSTERED INDEX IX_DV_Producto
        ON DetalleVentas (ProductoID);
END
GO



