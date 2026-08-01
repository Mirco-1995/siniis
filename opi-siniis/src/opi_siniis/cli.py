from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Annotated, Optional

import typer
from loguru import logger

from opi_siniis.constants import load_properties
from opi_siniis.core import (
    LoadResult,
    MIN_RECORD_LENGTH,
    OracleSiniisLoader,
    ParseResult,
    SiniisRecord,
    parse_line,
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


def merge_load_result(total: LoadResult, partial: LoadResult):
    total.total_lines += partial.total_lines
    total.loaded += partial.loaded
    total.skipped += partial.skipped
    total.errors.extend(partial.errors)


def load_chunk(
    loader: OracleSiniisLoader,
    chunk: list[SiniisRecord],
    rata: int,
    chunk_number: int,
    truncate_partition: bool,
    record_number_offset: int,
) -> LoadResult:
    logger.info(f"Caricamento blocco {chunk_number}: {len(chunk)} record validi")
    result = loader.load_records(
        chunk,
        rata,
        truncate_partition=truncate_partition,
        record_number_offset=record_number_offset,
    )
    logger.info(
        f"Blocco {chunk_number} completato: "
        f"{result.loaded} caricati, {result.skipped} scartati DB"
    )
    return result


def file_has_line_separators(file_path: Path) -> bool:
    with open(file_path, "rb") as input_file:
        while True:
            chunk = input_file.read(64 * 1024)
            if not chunk:
                return False
            if b"\n" in chunk or b"\r" in chunk:
                return True


def build_length_error(line: bytes, line_number: int) -> ParseResult:
    return ParseResult(
        success=False,
        error=f"Lunghezza record non valida: attesi almeno {MIN_RECORD_LENGTH} byte, trovata {len(line)}",
        line_number=line_number,
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
    chunk_number = 1
    partition_prepared = False
    chunk: list[SiniisRecord] = []
    load_result = LoadResult()
    loader: OracleSiniisLoader | None = None

    try:
        logger.info(f"Parsing file: {file_path}")
        has_line_separators = file_has_line_separators(file_path)

        with open(file_path, "rb") as input_file:
            while True:
                if has_line_separators:
                    line = input_file.readline()
                    if not line:
                        break
                    line = line.rstrip(b"\r\n")
                else:
                    line = input_file.read(MIN_RECORD_LENGTH)
                    if not line:
                        break

                total_lines += 1

                if len(line) < MIN_RECORD_LENGTH:
                    result = build_length_error(line, total_lines)
                else:
                    result = parse_line(line, rata_value, total_lines)

                if result.success and result.record:
                    chunk.append(result.record)
                    valid_records += 1
                else:
                    parse_errors_count += 1
                    logger.warning(f"Scartata riga {result.line_number}: {result.error}")
                    continue

                if len(chunk) >= LOAD_CHUNK_SIZE:
                    if loader is None:
                        loader = OracleSiniisLoader()

                    partial_result = load_chunk(
                        loader=loader,
                        chunk=chunk,
                        rata=rata_value,
                        chunk_number=chunk_number,
                        truncate_partition=not partition_prepared,
                        record_number_offset=load_result.total_lines,
                    )
                    merge_load_result(load_result, partial_result)

                    partition_prepared = True
                    chunk = []
                    chunk_number += 1

        if chunk:
            if loader is None:
                loader = OracleSiniisLoader()

            partial_result = load_chunk(
                loader=loader,
                chunk=chunk,
                rata=rata_value,
                chunk_number=chunk_number,
                truncate_partition=not partition_prepared,
                record_number_offset=load_result.total_lines,
            )
            merge_load_result(load_result, partial_result)
            partition_prepared = True

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
