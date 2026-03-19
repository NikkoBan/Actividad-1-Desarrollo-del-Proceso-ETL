"""Modelo de dominio para Customer."""

from dataclasses import dataclass


@dataclass
class Customer:
    customer_id: int
    first_name: str
    last_name: str
    email: str
    phone: str
    city: str
    country: str
