from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Annotated, Optional

import typer
from loguru import logger

from opi_siniis.constants import SINIIS_PG_FILE_PATH
from opi_siniis.core import (
    LoadResult,
    OracleSiniisLoader,
    parse_file,
)

app = typer.Typer(add_completion=False)


def setup_logging(verbose: bool = False):
    logger.remove()
    level = "DEBUG" if verbose else "INFO"
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level=level,
    )


def validate_rata(rata: int) -> bool:
    if rata < 200001 or rata > 209912:
        return False
    month = rata % 100
    if month < 1 or month > 12:
        return False
    return True


def resolve_file_path(file_param: Optional[str]) -> Path:
    if file_param:
        file_path = Path(file_param)
    elif SINIIS_PG_FILE_PATH:
        file_path = Path(SINIIS_PG_FILE_PATH)
    else:
        raise typer.BadParameter(
            "Nessun path file specificato. "
            "Usa --file oppure configura SINIIS_PG_FILE_PATH in .env"
        )

    if not file_path.exists():
        raise typer.BadParameter(f"File non trovato: {file_path}")
    if not file_path.is_file():
        raise typer.BadParameter(f"Il path non è un file: {file_path}")
    if not os.access(file_path, os.R_OK):
        raise typer.BadParameter(f"File non leggibile: {file_path}")

    return file_path


@app.command()
def run(
    file: Annotated[
        Optional[str],
        typer.Option(
            "--file", "-f",
            help="Path assoluto del file siniis_pg"
        ),
    ] = None,
    rata: Annotated[
        int,
        typer.Option(
            "--rata", "-r",
            help="Rata versamento in formato YYYYMM"
        ),
    ] = ...,
    verbose: Annotated[
        bool,
        typer.Option(
            "--verbose", "-v",
            help="Abilita logging dettagliato"
        ),
    ] = False,
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Esegue solo parsing senza caricare su Oracle"
        ),
    ] = False,
):
    setup_logging(verbose)

    if not validate_rata(rata):
        logger.critical(f"Rata non valida: {rata}. Formato atteso: YYYYMM")
        raise typer.Exit(code=1)

    try:
        file_path = resolve_file_path(file)
    except typer.BadParameter as e:
        logger.critical(str(e))
        raise typer.Exit(code=1)

    logger.info(f"File siniis_pg: {file_path}")
    logger.info(f"Rata versamento: {rata}")

    records = []
    parse_errors = []
    total_lines = 0

    for result in parse_file(file_path, rata):
        total_lines += 1
        if result.success and result.record:
            records.append(result.record)
        else:
            parse_errors.append(f"Riga {result.line_number}: {result.error}")
            if verbose:
                logger.warning(f"Scartata riga {result.line_number}: {result.error}")

    logger.info(f"Parsing completato: {len(records)}/{total_lines} record validi")

    if parse_errors:
        logger.warning(f"Record scartati in parsing: {len(parse_errors)}")

    if not records:
        logger.warning("Nessun record valido da caricare")
        raise typer.Exit(code=0)

    if dry_run:
        logger.info("[DRY-RUN] Nessun caricamento effettuato")
        raise typer.Exit(code=0)

    try:
        loader = OracleSiniisLoader()

        if not loader.ensure_partition(rata):
            logger.critical(f"Impossibile garantire partizione P_{rata}")
            raise typer.Exit(code=1)

        load_result = loader.load_records(records, rata)

        logger.info("=" * 50)
        logger.info("REPORT CARICAMENTO")
        logger.info(f"  Righe lette:     {total_lines}")
        logger.info(f"  Record validi:   {len(records)}")
        logger.info(f"  Caricati:        {load_result.loaded}")
        logger.info(f"  Scartati parse:  {len(parse_errors)}")
        logger.info(f"  Scartati DB:     {load_result.skipped}")
        logger.info("=" * 50)

        if load_result.errors:
            logger.warning("Errori DB:")
            for err in load_result.errors[:10]:
                logger.warning(f"  - {err}")
            if len(load_result.errors) > 10:
                logger.warning(f"  ... e altri {len(load_result.errors) - 10} errori")

        if load_result.loaded == 0:
            logger.error("Nessun record caricato!")
            raise typer.Exit(code=1)

        logger.success(f"Caricamento completato: {load_result.loaded} record")
        raise typer.Exit(code=0)

    except RuntimeError as e:
        logger.critical(str(e))
        raise typer.Exit(code=1)

    except Exception as e:
        logger.critical(f"Errore imprevisto: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
