from __future__ import annotations

from pathlib import Path
import typer
from loguru import logger

# Permette l'esecuzione diretta da dentro la cartella gestione_siniis
_PARENT = Path(__file__).resolve().parent.parent
import sys

if str(_PARENT) not in sys.path:
    sys.path.insert(0, str(_PARENT))

from gestione_siniis.core import (  # noqa: E402
    FlowType,
    MongoPgResolver,
    OracleSiniisWriter,
    OracleSpendingCodesProvider,
    SpendingType,
    aggregate_siniis,
    find_siniis_files,
)


def main(
        rata: int = typer.Option(..., "-r", "--rata", help="Rata in formato AAAAMM"),
        flow_type: str = typer.Option(FlowType.DEDUCTIONS, "-t", "--tipo-flusso"),
        spending_type: str = typer.Option(SpendingType.SPT, "-s", "--tipo-spesa"),
        verbose: bool = typer.Option(False, "--verbose", "-v"),
):
    logger.remove()
    logger.add(lambda msg: print(msg, end=""), level="DEBUG" if verbose else "INFO")

    files = find_siniis_files(rata, flow_type, spending_type)
    if not files:
        logger.warning("Nessun file SINIIS trovato")
        raise typer.Exit(code=0)

    resolver = MongoPgResolver(rata_versamento=str(rata))
    allowed_codes = OracleSpendingCodesProvider().load_codes()
    writer = OracleSiniisWriter()

    for file_path in files:
        result = aggregate_siniis(file_path, str(rata), resolver, allowed_codes)
        writer.write(result.rows, str(rata))
        logger.info(f"File {file_path.name}: aggregate {len(result.rows)} righe")


if __name__ == "__main__":
    typer.run(main)
