"""Modelos de dominio del sistema ETL."""

from .customer import Customer
from .order import Order
from .order_detail import OrderDetail
from .product import Product

__all__ = ["Customer", "Order", "OrderDetail", "Product"]
