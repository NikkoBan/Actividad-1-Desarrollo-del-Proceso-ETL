"""Módulo de configuración del sistema ETL."""

import json
import os
from pathlib import Path

from dotenv import load_dotenv


class Settings:
    """Gestiona la configuración del sistema desde config.json y variables de entorno."""

    def __init__(self, config_path: str | None = None):
        load_dotenv()

        if config_path is None:
            config_path = str(Path(__file__).parent / "config.json")

        with open(config_path, "r", encoding="utf-8") as f:
            self._config = json.load(f)

        # Variables de entorno sobreescriben config.json
        self.db_connection_string = os.getenv(
            "DB_CONNECTION_STRING",
            self._config["database"]["oltp_connection_string"],
        )
        self.dw_connection_string = os.getenv(
            "DW_CONNECTION_STRING",
            self._config["database"]["dw_connection_string"],
        )
        self.api_base_url = os.getenv(
            "API_BASE_URL",
            self._config["api"]["base_url"],
        )
        self.api_endpoints: dict[str, str] = self._config["api"]["endpoints"]
        self.api_retry_max = self._config["api"]["retry"]["max_attempts"]
        self.api_retry_delay = self._config["api"]["retry"]["delay_seconds"]

        base = Path(__file__).parent.parent
        self.csv_folder = str(base / self._config["paths"]["csv_folder"])
        self.staging_folder = str(base / self._config["paths"]["staging_folder"])
        self.logs_folder = str(base / self._config["paths"]["logs_folder"])

        self.batch_size = self._config["etl"]["batch_size"]
        self.parallel_workers = self._config["etl"]["parallel_workers"]
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
