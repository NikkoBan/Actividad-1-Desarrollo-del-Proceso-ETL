"""TraceManager - Sistema de logging, monitoreo y métricas de ejecución.

Registra eventos en archivo y consola con timestamps,
mide tiempos de ejecución y genera métricas simples.
"""

import logging
import time
from pathlib import Path


class TraceManager:
    """Gestiona trazabilidad, logs y métricas del proceso ETL."""

    def __init__(self, logs_folder: str, log_level: str = "INFO"):
        self._logs_path = Path(logs_folder)
        self._logs_path.mkdir(parents=True, exist_ok=True)

        self._metrics: dict[str, float] = {}
        self._timers: dict[str, float] = {}
        self._counters: dict[str, int] = {}

        self._logger = logging.getLogger("ETL")
        self._logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        self._logger.handlers.clear()

        # Handler archivo
        log_file = self._logs_path / "etl.log"
        fh = logging.FileHandler(str(log_file), encoding="utf-8")
        fh.setLevel(logging.DEBUG)

        # Handler consola
        ch = logging.StreamHandler()
        ch.setLevel(getattr(logging, log_level.upper(), logging.INFO))

        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        fh.setFormatter(fmt)
        ch.setFormatter(fmt)

        self._logger.addHandler(fh)
        self._logger.addHandler(ch)

    def info(self, message: str) -> None:
        self._logger.info(message)

    def error(self, message: str, exc: Exception | None = None) -> None:
        if exc:
            self._logger.error(f"{message}: {exc}", exc_info=True)
        else:
            self._logger.error(message)

    def warning(self, message: str) -> None:
        self._logger.warning(message)

    def debug(self, message: str) -> None:
        self._logger.debug(message)

    # ── Métricas de tiempo ──────────────────────────────────────
    def start_timer(self, label: str) -> None:
        self._timers[label] = time.perf_counter()
        self.info(f"⏱  Inicio: {label}")

    def stop_timer(self, label: str) -> float:
        if label not in self._timers:
            self.warning(f"Timer '{label}' no fue iniciado")
            return 0.0
        elapsed = time.perf_counter() - self._timers.pop(label)
        self._metrics[label] = elapsed
        self.info(f"⏱  Fin: {label} → {elapsed:.3f}s")
        return elapsed

    # ── Contadores ──────────────────────────────────────────────
    def increment(self, counter: str, amount: int = 1) -> None:
        self._counters[counter] = self._counters.get(counter, 0) + amount

    def get_counter(self, counter: str) -> int:
        return self._counters.get(counter, 0)

    # ── Resumen ─────────────────────────────────────────────────
    def print_summary(self) -> None:
        self.info("=" * 60)
        self.info("📊  RESUMEN DE EJECUCIÓN ETL")
        self.info("=" * 60)
        if self._metrics:
            self.info("Tiempos:")
            for label, elapsed in self._metrics.items():
                self.info(f"  • {label}: {elapsed:.3f}s")
            total = sum(self._metrics.values())
            self.info(f"  ► Total: {total:.3f}s")
        if self._counters:
            self.info("Contadores:")
            for name, value in self._counters.items():
                self.info(f"  • {name}: {value}")
        self.info("=" * 60)
