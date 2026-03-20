"""DTOs (Data Transfer Objects) con validación Pydantic."""

from .customer_dto import CustomerDTO
from .order_detail_dto import OrderDetailDTO
from .order_dto import OrderDTO
from .product_dto import ProductDTO
from .api_dto import UserDTO, PostDTO, CommentDTO
from .staging_dto import (
    StgClienteDTO,
    StgProductoDTO,
    StgVentaDTO,
    StgDetalleVentaDTO,
    StgApiComentarioDTO,
)

__all__ = [
    "CustomerDTO",
    "OrderDTO",
    "OrderDetailDTO",
    "ProductDTO",
    "UserDTO",
    "PostDTO",
    "CommentDTO",
    "StgClienteDTO",
    "StgProductoDTO",
    "StgVentaDTO",
    "StgDetalleVentaDTO",
    "StgApiComentarioDTO",
]

