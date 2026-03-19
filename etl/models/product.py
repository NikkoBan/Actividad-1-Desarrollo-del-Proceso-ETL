"""Modelo de dominio para Product."""

from dataclasses import dataclass


@dataclass
class Product:
    product_id: int
    product_name: str
    category: str
    price: float
    stock: int
