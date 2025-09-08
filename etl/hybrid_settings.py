# -*- coding: utf-8 -*-
"""
Hybrid settings loader:
* Loads .env (via python-dotenv) → provides secrets.
* Loads config.ini → provides static defaults, sections, and type-casting.
* Exposes a single pydantic Settings object for the rest of the code.
"""

from __future__ import annotations

import os
from pathlib import Path
import configparser

# --------------------------------------------------------------
# 1️⃣ Load .env – file lives inside `etl/.env`
# --------------------------------------------------------------
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]          # repo root (../)
load_dotenv(dotenv_path=ROOT / "etl" / ".env")      # populates os.environ

# --------------------------------------------------------------
# 2️⃣ Load INI defaults
# --------------------------------------------------------------
INI_PATH = ROOT / "etl" / "config.ini"
ini = configparser.ConfigParser()
ini.read(INI_PATH)


# helper to strip inline comments like "; raw CSVs"
def _clean(value: str) -> str:
    return value.split(";")[0].strip() if value else value


# --------------------------------------------------------------
# 3️⃣ Settings class – values are taken from OS env (including .env)
#    and fall back to the INI defaults.
# --------------------------------------------------------------
from pydantic import Field, validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # ----------- DB ------------
    DB_DIALECT: str = Field(default=_clean(ini.get("db", "dialect")))
    DB_HOST: str = Field(default=_clean(ini.get("db", "host")))
    DB_PORT: int = Field(default=ini.getint("db", "port"))
    DB_SERVICE: str = Field(default=_clean(ini.get("db", "service_name")))
    DB_USER: str = Field(default=_clean(ini.get("db", "user")))

    # Secret – must come from environment (.env)
    DB_PASSWORD: str = Field(..., env="DB_PASSWORD")

    # ----------- Paths ----------
    DATA_PATH: Path = Field(default=Path(_clean(ini.get("DEFAULT", "data_path"))))
    STAGING_PATH: Path = Field(default=Path(_clean(ini.get("DEFAULT", "staging_path"))))

    # ----------- ETL options -----
    LOG_LEVEL: str = Field(
        default=_clean(ini.get("DEFAULT", "log_level", fallback="INFO"))
    )
    BATCH_SIZE: int = Field(
        default=ini.getint("etl", "batch_size", fallback=5000)
    )

    # ----------------------------------------------------------
    # Validators
    # ----------------------------------------------------------
    @validator("LOG_LEVEL")
    def _valid_log_level(cls, v: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_up = v.upper()
        if v_up not in allowed:
            raise ValueError(f"LOG_LEVEL must be one of {allowed}")
        return v_up

    # ----------------------------------------------------------
    # Pydantic 2 config
    # ----------------------------------------------------------
    model_config = {
        "env_prefix": "",
        "env_file": str(ROOT / "etl" / ".env"),   # consistent with load_dotenv
        "case_sensitive": False,
    }


# --------------------------------------------------------------
# Export a singleton for easy import elsewhere
# --------------------------------------------------------------
settings = Settings()   # validated on import – raises if required vars missing
