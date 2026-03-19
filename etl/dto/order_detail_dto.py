"""DTO para OrderDetail con validación Pydantic."""

from pydantic import BaseModel, Field


class OrderDetailDTO(BaseModel):
    order_id: int = Field(..., gt=0, alias="OrderID")
    product_id: int = Field(..., gt=0, alias="ProductID")
    quantity: int = Field(..., gt=0, alias="Quantity")
    total_price: float = Field(..., ge=0, alias="TotalPrice")

    model_config = {"populate_by_name": True}
