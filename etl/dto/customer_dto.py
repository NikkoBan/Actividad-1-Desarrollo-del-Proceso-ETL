"""DTO para Customer con validación Pydantic."""

from pydantic import BaseModel, EmailStr, Field


class CustomerDTO(BaseModel):
    customer_id: int = Field(..., gt=0, alias="CustomerID")
    first_name: str = Field(..., min_length=1, alias="FirstName")
    last_name: str = Field(..., min_length=1, alias="LastName")
    email: str = Field(..., alias="Email")
    phone: str = Field(..., alias="Phone")
    city: str = Field(..., alias="City")
    country: str = Field(..., alias="Country")

    model_config = {"populate_by_name": True}

