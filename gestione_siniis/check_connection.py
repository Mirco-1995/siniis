"""
Connectivity checks for MongoDB and Oracle using credentials from constants.py.

Usage examples:
  .venv\\Scripts\\python.exe check_connections.py mongo
  .venv\\Scripts\\python.exe check_connections.py oracle
  .venv\\Scripts\\python.exe check_connections.py all
"""

from __future__ import annotations

import typer
from pymongo import MongoClient

from constants import (
    MONGO_DB,
    MONGO_URI,
    ORACLE_DSN,
    ORACLE_PASS,
    ORACLE_USER,
)

try:
    import oracledb  # type: ignore
except ImportError:  # pragma: no cover - handled at runtime
    oracledb = None  # type: ignore

app = typer.Typer(add_completion=False)


def _check_mongo(verbose: bool = True) -> bool:
    client = None
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client.get_database(MONGO_DB)
        db.list_collection_names()
        if verbose:
            typer.secho(
                f"MongoDB connection OK (database '{MONGO_DB}' reachable).",
                fg="green",
            )
        return True
    except Exception as exc:  # pragma: no cover - depends on runtime environment
        if verbose:
            typer.secho(f"MongoDB connection FAILED: {exc}", fg="red", err=True)
        return False
    finally:
        if client is not None:
            client.close()


def _check_oracle(verbose: bool = True) -> bool:
    if oracledb is None:
        if verbose:
            typer.secho(
                "Oracle driver (oracledb) non disponibile: installalo per eseguire il check.",
                fg="red",
                err=True,
            )
        return False

    conn = None
    try:
        conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=ORACLE_DSN)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM dual")
        cur.fetchone()
        cur.close()
        if verbose:
            typer.secho("Oracle connection OK (SELECT 1 eseguito).", fg="green")
        return True
    except Exception as exc:  # pragma: no cover - depends on runtime environment
        if verbose:
            typer.secho(f"Oracle connection FAILED: {exc}", fg="red", err=True)
        return False
    finally:
        if conn is not None:
            conn.close()


@app.command()
def mongo() -> None:
    """Verifica la connessione verso MongoDB."""
    ok = _check_mongo()
    raise typer.Exit(code=0 if ok else 1)


@app.command()
def oracle() -> None:
    """Verifica la connessione verso Oracle."""
    ok = _check_oracle()
    raise typer.Exit(code=0 if ok else 1)


@app.command("all")
def check_all() -> None:
    """Verifica entrambe le connessioni, MongoDB e Oracle."""
    ok_mongo = _check_mongo()
    ok_oracle = _check_oracle()
    if ok_mongo and ok_oracle:
        typer.secho("Tutte le connessioni sono OK.", fg="green")
        raise typer.Exit(code=0)
    typer.secho("Alcune connessioni hanno riportato errori.", fg="red", err=True)
    raise typer.Exit(code=1)


if __name__ == "__main__":
    app()

