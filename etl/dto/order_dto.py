"""DTO para Order con validación Pydantic."""

from pydantic import BaseModel, Field


class OrderDTO(BaseModel):
    order_id: int = Field(..., gt=0, alias="OrderID")
    customer_id: int = Field(..., gt=0, alias="CustomerID")
    order_date: str = Field(..., alias="OrderDate")
    status: str = Field(..., alias="Status")

    model_config = {"populate_by_name": True}

