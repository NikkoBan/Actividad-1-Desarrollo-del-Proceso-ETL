"""DTO para Product con validación Pydantic."""

from pydantic import BaseModel, Field


class ProductDTO(BaseModel):
    product_id: int = Field(..., gt=0, alias="ProductID")
    product_name: str = Field(..., min_length=1, alias="ProductName")
    category: str = Field(..., alias="Category")
    price: float = Field(..., ge=0, alias="Price")
    stock: int = Field(..., ge=0, alias="Stock")

    model_config = {"populate_by_name": True}

