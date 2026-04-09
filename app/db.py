import sqlite3
from datetime import datetime, timezone

from app.config import DATABASE_PATH, SCHEMA_PATH


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_connection():
    connection = sqlite3.connect(DATABASE_PATH, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def init_db():
    connection = get_connection()
    try:
        with open(SCHEMA_PATH, "r", encoding="utf-8") as schema_file:
            connection.executescript(schema_file.read())
        connection.commit()
    finally:
        connection.close()
