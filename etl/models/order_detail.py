"""Modelo de dominio para OrderDetail."""

from dataclasses import dataclass


@dataclass
class OrderDetail:
    order_id: int
    product_id: int
    quantity: int
    total_price: float
