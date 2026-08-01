from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Annotated, Optional

import typer
from loguru import logger

from opi_siniis.constants import load_properties
from opi_siniis.core import (
    OracleSiniisLoader,
    SiniisRecord,
    parse_file,
)

app = typer.Typer(add_completion=False)

LOAD_CHUNK_SIZE = 5_000_000


def setup_logging():
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO",
    )


def validate_rata(rata: int) -> bool:
    if rata < 200001 or rata > 209912:
        return False
    month = rata % 100
    if month < 1 or month > 12:
        return False
    return True


def resolve_file_path(file_param: Optional[str], props: dict) -> Path:
    if file_param:
        file_path = Path(file_param)
    elif props.get("siniis_pg.path"):
        file_path = Path(props["siniis_pg.path"])
    else:
        raise typer.BadParameter(
            "Nessun path file specificato. "
            "Usa --file oppure configura siniis_pg.path nel file properties"
        )

    if not file_path.exists():
        raise typer.BadParameter(f"File non trovato: {file_path}")
    if not file_path.is_file():
        raise typer.BadParameter(f"Il path non è un file: {file_path}")
    if not os.access(file_path, os.R_OK):
        raise typer.BadParameter(f"File non leggibile: {file_path}")

    return file_path


def resolve_rata(rata_param: Optional[int], props: dict) -> int:
    if rata_param:
        return rata_param
    if props.get("rata_versamento"):
        try:
            return int(props["rata_versamento"])
        except ValueError:
            raise typer.BadParameter(
                f"rata_versamento nel file properties non valido: {props['rata_versamento']}"
            )
    raise typer.BadParameter(
        "Nessuna rata specificata. "
        "Usa --rata oppure configura rata_versamento nel file properties"
    )


@app.command()
def run(
    file: Annotated[
        Optional[str],
        typer.Option(
            "--file",
            help="Path assoluto del file siniis_pg"
        ),
    ] = None,
    rata: Annotated[
        Optional[int],
        typer.Option(
            "--rata",
            help="Rata versamento in formato YYYYMM"
        ),
    ] = None,
    props: Annotated[
        Optional[str],
        typer.Option(
            "--props",
            help="Path alternativo del file di properties"
        ),
    ] = None,
):
    setup_logging()

    properties = load_properties(props)

    try:
        rata_value = resolve_rata(rata, properties)
    except typer.BadParameter as e:
        logger.critical(str(e))
        raise typer.Exit(code=1)

    if not validate_rata(rata_value):
        logger.critical(f"Rata non valida: {rata_value}. Formato atteso: YYYYMM")
        raise typer.Exit(code=1)

    try:
        file_path = resolve_file_path(file, properties)
    except typer.BadParameter as e:
        logger.critical(str(e))
        raise typer.Exit(code=1)

    logger.info(f"File siniis_pg: {file_path}")
    logger.info(f"Rata versamento: {rata_value}")

    total_lines = 0
    valid_records = 0
    parse_errors_count = 0

    def iter_record_chunks():
        nonlocal total_lines, valid_records, parse_errors_count

        chunk: list[SiniisRecord] = []
        chunk_number = 1

        for result in parse_file(file_path, rata_value):
            total_lines += 1
            if result.success and result.record:
                chunk.append(result.record)
                valid_records += 1

                if len(chunk) >= LOAD_CHUNK_SIZE:
                    logger.info(
                        f"Blocco {chunk_number} pronto: {len(chunk)} record validi "
                        f"su {total_lines} righe lette"
                    )
                    yield chunk
                    chunk = []
                    chunk_number += 1
            else:
                parse_errors_count += 1
                logger.warning(f"Scartata riga {result.line_number}: {result.error}")

        if chunk:
            logger.info(
                f"Blocco {chunk_number} pronto: {len(chunk)} record validi "
                f"su {total_lines} righe lette"
            )
            yield chunk

    try:
        loader = OracleSiniisLoader()
        load_result = loader.load_record_chunks(iter_record_chunks(), rata_value)

        logger.info(f"Parsing completato: {valid_records}/{total_lines} record validi")

        if parse_errors_count:
            logger.warning(f"Record scartati in parsing: {parse_errors_count}")

        if valid_records == 0:
            logger.warning("Nessun record valido da caricare")
            raise typer.Exit(code=0)

        logger.info("=" * 50)
        logger.info("REPORT CARICAMENTO")
        logger.info(f"  Righe lette:     {total_lines}")
        logger.info(f"  Record validi:   {valid_records}")
        logger.info(f"  Caricati:        {load_result.loaded}")
        logger.info(f"  Scartati parse:  {parse_errors_count}")
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
        return

    except typer.Exit:
        raise

    except RuntimeError as e:
        logger.critical(str(e))
        raise typer.Exit(code=1)

    except Exception as e:
        logger.critical(f"Errore imprevisto: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
