"""DTOs para las tablas de staging de la base de datos analítica."""

from pydantic import BaseModel, Field


class StgClienteDTO(BaseModel):
    cliente_id: str
    nombre: str
    apellido: str
    email: str | None = None
    telefono: str | None = None
    ciudad: str | None = None
    pais: str | None = None
    segmento: str | None = Field(default="General")
    fuente: str = "CSV"


class StgProductoDTO(BaseModel):
    producto_id: str
    nombre: str
    categoria: str | None = None
    precio: str
    stock: str | None = None
    fuente: str = "CSV"


class StgVentaDTO(BaseModel):
    venta_id: str
    cliente_id: str
    fecha_venta: str
    estado: str | None = None
    fuente: str = "CSV"


class StgDetalleVentaDTO(BaseModel):
    venta_id: str
    producto_id: str
    cantidad: str
    precio_unitario: str | None = None
    total_linea: str
    fuente: str = "CSV"


class StgApiComentarioDTO(BaseModel):
    post_id: str
    comentario_id: str
    nombre: str | None = None
    email: str | None = None
    cuerpo: str | None = None
    fuente: str = "API"
