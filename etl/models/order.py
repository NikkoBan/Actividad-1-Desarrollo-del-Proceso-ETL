"""Modelo de dominio para Order."""

from dataclasses import dataclass


@dataclass
class Order:
    order_id: int
    customer_id: int
    order_date: str
    status: str
