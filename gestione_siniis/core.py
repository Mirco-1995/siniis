from __future__ import annotations

import calendar
import csv
import re
import tempfile
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Protocol

from loguru import logger

from gestione_siniis.constants import REMOTE_DIR_FILES
try:
    import oracledb
except ImportError:  # pragma: no cover
    oracledb = None

try:
    from pymongo import MongoClient
except ImportError:  # pragma: no cover
    MongoClient = None


class FlowType(str):
    ORDINARY = "ORDINARIA"
    SPECIAL = "SPECIALE"
    DEDUCTIONS = "RITENUTE"


class SpendingType(str):
    SPT = "SPT"
    PDG = "PDG"


SINIIS_REGEX = re.compile(r"[sS][iI][nN][iI][iI][sS].*")


# Mapping per valori negativi: la lettera finale codifica l'ultima cifra e il segno
NEGATIVE_ZONED_MAP = {
    "p": "0",
    "q": "1",
    "r": "2",
    "s": "3",
    "t": "4",
    "u": "5",
    "v": "6",
    "w": "7",
    "x": "8",
    "y": "9",
}


@dataclass
class AggregatedRow:
    rata_versamento: str
    cod_rit: str
    num_zona: str
    cod_cspesa: str
    capitolo_bil_stato: str
    cod_ente: int
    num_pg: str
    importo: int


@dataclass
class SiniisAggregationResult:
    rows: list[AggregatedRow]
    file_path: Path


class PgResolver(Protocol):
    def resolve(self, iscrizione: str) -> tuple[int | None, str | None]:
        ...


def parse_cobol_int(raw_value: str) -> int:
    """
    Converte un numero COBOL (zoned, 2 decimali) in intero di centesimi.
    """
    raw_value = raw_value.strip()
    if not raw_value:
        return 0

    last_char = raw_value[-1]
    sign = 1
    if last_char.lower() in NEGATIVE_ZONED_MAP:
        sign = -1
        mapped_digit = NEGATIVE_ZONED_MAP[last_char.lower()]
        numeric = f"{raw_value[:-1]}{mapped_digit}"
    else:
        numeric = raw_value

    if not numeric.isdigit():
        raise ValueError(f"Valore importo non riconosciuto: {raw_value}")

    return int(numeric) * sign


def _decode_field(raw: bytes) -> str:
    return raw.decode("latin-1", errors="ignore").strip()


@dataclass
class _ParsedSiniisLine:
    cod_rit: str
    num_zona: str
    cod_cspesa: str
    capitolo_bil_stato: str
    iscrizione: str
    importo: int
    tipo_zona: str
    data_trattamento: str
    provenienza: str


def _parse_line(line: bytes) -> _ParsedSiniisLine:
    cod_rit = _decode_field(line[2:5])  # POSITION(03:05)
    cod_cspesa = _decode_field(line[10:14])  # POSITION(11:14)
    capitolo_bil_stato = _decode_field(line[14:18])  # POSITION(15:18)
    iscrizione = int(_decode_field(line[18:26]))  # POSITION(19:26)
    importo_raw = _decode_field(line[33:41])  # POSITION(34:41)
    tipo_zona = _decode_field(line[5:6])  # POSITION(06:06)
    data_trattamento = _decode_field(line[41:49])  # POSITION(42:49)
    provenienza = _decode_field(line[57:58])  # POSITION(58:58)
    if tipo_zona == "L":
        num_zona = _decode_field(line[26:30])  # POSITION(27:30)
    else:
        num_zona = _decode_field(line[6:10])  # POSITION(07:10)
    importo = parse_cobol_int(importo_raw)
    return _ParsedSiniisLine(
        cod_rit=cod_rit,
        num_zona=num_zona,
        cod_cspesa=cod_cspesa,
        capitolo_bil_stato=capitolo_bil_stato,
        iscrizione=iscrizione,
        importo=importo,
        tipo_zona=tipo_zona,
        data_trattamento=data_trattamento,
        provenienza=provenienza,
    )


def _select_latest_document(documents: Iterable[dict]) -> dict | None:
    latest_document = None
    for document in documents:
        if latest_document is None:
            latest_document = document
            continue
        if document.get("rataRiferimento", "") > latest_document.get("rataRiferimento", ""):
            latest_document = document
    return latest_document


def _extract_pg_and_ente(document: dict) -> tuple[int | None, str | None]:
    cod_ente = document.get("codiceEnte", None)
    if isinstance(cod_ente, str) and cod_ente.isdigit():
        cod_ente = int(cod_ente)
    num_pg = document.get("pgNettiCalcolato")
    return cod_ente, num_pg


class MongoPgResolver:
    """
    Recupera il numero di partita e il codice ente interrogando MongoDB.
    """

    def __init__(self,
                 rata_versamento: str,
                 uri: str | None = None,
                 database_name: str | None = None,
                 collection_name: str | None = None) -> None:
        if MongoClient is None:
            raise ImportError("pymongo non installato: aggiungi la dependency per usare SINIIS")
        self._uri = uri or os.environ.get("MONGODB_URI")
        if not self._uri:
            raise EnvironmentError("Variabile d'ambiente non definita: MONGODB_URI")
        self._database_name = database_name or os.environ.get("MONGODB_DB", "opi-int")
        self._collection_name = collection_name or os.environ.get("MONGODB_COLLECTION", "flussoEMISTI")
        self._rata_versamento = rata_versamento

        logger.debug(f"Inizializzo MongoDB client su {self._uri}, db {self._database_name}, collection {self._collection_name}")
        client = MongoClient(self._uri)
        self._collection = client[self._database_name][self._collection_name]

    def resolve(self, iscrizione: str, capitolo: str, codice_spesa: str) -> tuple[int | None, str | None]:
        try:
            iscrizione_val = int(iscrizione)
        except ValueError:
            iscrizione_val = iscrizione
        rata_str = str(self._rata_versamento)

        capitolo_candidates = str(capitolo)
#        try:
#            capitolo_int = int(capitolo)
#            capitolo_candidates.append(capitolo_int)
#        except ValueError:
#            pass

        codice_spesa_candidates = int(codice_spesa)
#        try:
#            codice_spesa_int = int(codice_spesa)
#            codice_spesa_candidates.append(codice_spesa_int)
#        except ValueError:
#            pass

        query = {
            "iscrizione": iscrizione_val,
            "rataEmissione": rata_str,
            "capitoloDiBilancio": capitolo_candidates,
            "codiceSpesa": codice_spesa_candidates,
        }
        print(query)

        documents = list(self._collection.find(query))
        if not documents:
            logger.warning(f"Nessun documento Mongo trovato per iscrizione={iscrizione_val}, rata={rata_str}, capitolo={capitolo}, codice_spesa={codice_spesa}: uso PG 01")
            return None, "01"
        latest_document = _select_latest_document(documents)
        if not latest_document:
            logger.warning(f"Nessun documento valido per iscrizione={iscrizione_val}, rata={rata_str}, capitolo={capitolo}, codice_spesa={codice_spesa}: uso PG 01")
            return None, "01"
        cod_ente, num_pg = _extract_pg_and_ente(latest_document)
        if num_pg is None:
            logger.warning(f"pgNettiCalcolato mancante per iscrizione={iscrizione_val}, rata={rata_str}: uso PG 01")
            num_pg = "01"
        return cod_ente, num_pg


class OracleSpendingCodesProvider:
    """
    Recupera i codici spesa ammessi interrogando Oracle e li associa all'ente.
    """

    def __init__(self,
                 dsn: str | None = None,
                 user: str | None = None,
                 password: str | None = None) -> None:
        if oracledb is None:
            raise ImportError("oracledb non installato: aggiungi la dependency per usare SINIIS")

        self._dsn = dsn or os.environ.get("ORACLE_DSN")
        self._user = user or os.environ.get("ORACLE_USER")
        self._password = password or os.environ.get("ORACLE_PASSWORD")
        if not all((self._dsn, self._user, self._password)):
            raise EnvironmentError("Variabili d'ambiente non definite: ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD")

    def load_codes(self) -> dict[str, int]:
        query = """
        SELECT T018_COD_CSPESA, T018_COD_ENTE
          FROM t018_capitoli_spesa
          WHERE T018_COD_ENTE IN (26, 41)
        """
        with oracledb.connect(user=self._user, password=self._password, dsn=self._dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                mapping = {str(row[0]).zfill(4): int(row[1]) for row in cur.fetchall()}
        logger.info(f"Recuperati {len(mapping)} codici spesa ammessi da Oracle")
        return mapping


class OracleSiniisWriter:
    """
    Scrive le righe aggregate nella tabella OPI_SINIIS_AGGREGATI.
    """

    def __init__(self,
                 dsn: str | None = None,
                 user: str | None = None,
                 password: str | None = None) -> None:
        if oracledb is None:
            raise ImportError("oracledb non installato: aggiungi la dependency per usare SINIIS")

        self._dsn = dsn or os.environ.get("ORACLE_DSN")
        self._user = user or os.environ.get("ORACLE_USER")
        self._password = password or os.environ.get("ORACLE_PASSWORD")
        if not all((self._dsn, self._user, self._password)):
            raise EnvironmentError("Variabili d'ambiente non definite: ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD")

    def write(self, rows: list[AggregatedRow], rata: str) -> None:
        if not rows:
            logger.info("Nessuna riga SINIIS da scrivere su Oracle")
            return

        logger.info(f"Avvio inserimento di {len(rows)} righe in SPTOWNER.OPI_SINIIS_AGGREGATI")
        insert_sql = """
        INSERT INTO SPTOWNER.OPI_SINIIS_AGGREGATI
            (RATA_VERSAMENTO, COD_RIT, NUM_ZONA, COD_CSPESA, CAPITOLO_BIL_STATO, COD_ENTE, NUM_PG, IMPORTO)
        VALUES
            (:1, :2, :3, :4, :5, :6, :7, :8)
        """
        binds = [
            (
                row.rata_versamento,
                row.cod_rit,
                row.num_zona,
                int(row.cod_cspesa),
                row.capitolo_bil_stato,
                row.cod_ente,
                row.num_pg,
                row.importo / 100,
            )
            for row in rows
        ]
        logger.debug(f"Connessione a Oracle DSN={self._dsn} user={self._user}")
        with oracledb.connect(user=self._user, password=self._password, dsn=self._dsn) as conn:
            with conn.cursor() as cur:
                logger.debug(f"Cancello eventuali dati esistenti per rata {rata}")
                cur.execute(
                    "DELETE FROM SPTOWNER.OPI_SINIIS_AGGREGATI WHERE RATA_VERSAMENTO = :1",
                    [rata],
                )
                logger.debug("Esecuzione inserimento bulk su Oracle")
                cur.executemany(insert_sql, binds)
            conn.commit()
        logger.success(f"Inserite {len(rows)} righe in SPTOWNER.OPI_SINIIS_AGGREGATI")


class _SiniisAggregator:
    def __init__(self, rata: str, resolver: PgResolver, allowed_cod_cspesa_map: dict[str, int]) -> None:
        self._installment = rata
        self._resolver = resolver
        self._allowed_cod_cspesa_map = allowed_cod_cspesa_map
        self._aggregates: dict[tuple, int] = {}
        self._cutoff_date = self._build_cutoff_date(rata)

    @staticmethod
    def _build_cutoff_date(installment: str) -> str:
        year = int(installment[:4])
        month = int(installment[4:6])
        last_day = calendar.monthrange(year, month)[1]
        return f"{installment}{last_day:02d}"

    def consume_file(self, file_path: Path) -> None:
        logger.info(f"Elaborazione file SINIIS {file_path}")
        with open(file_path, "rb") as file_in:
            for line_number, line in enumerate(file_in, start=1):
                if not line.strip():
                    continue
                try:
                    parsed_line = _parse_line(line)
                except Exception as exc:  # pragma: no cover
                    logger.warning(f"Ignoro riga {line_number}: parsing fallito ({exc})")
                    continue

                if parsed_line.importo == 0:
                    continue
                if parsed_line.cod_cspesa not in self._allowed_cod_cspesa_map:
                    logger.trace(f"Ignoro riga {line_number}: codice spesa {parsed_line.cod_cspesa} non ammesso")
                    continue
                cod_ente_from_cspesa = self._allowed_cod_cspesa_map[parsed_line.cod_cspesa]
                if parsed_line.data_trattamento == self._cutoff_date and parsed_line.provenienza != "M":
                    logger.trace(f"Ignoro riga {line_number}: data_trattamento={parsed_line.data_trattamento} non ammessa e provenienza={parsed_line.provenienza}")
                    continue

                _, num_pg = self._resolver.resolve(parsed_line.iscrizione, parsed_line.capitolo_bil_stato, parsed_line.cod_cspesa)
                if not num_pg:
                    logger.warning(f"Dati mancanti da MongoDB per iscrizione={parsed_line.iscrizione}: uso PG 01")
                    num_pg = "01"

                key = (
                    parsed_line.cod_rit,
                    parsed_line.num_zona,
                    parsed_line.cod_cspesa,
                    parsed_line.capitolo_bil_stato,
                    cod_ente_from_cspesa,
                    num_pg,
                )
                self._aggregates[key] = self._aggregates.get(key, 0) + parsed_line.importo

    def build_rows(self) -> list[AggregatedRow]:
        rows: list[AggregatedRow] = list()
        for key, total_importo in sorted(self._aggregates.items()):
            cod_rit, num_zona, cod_cspesa, capitolo_bil_stato, cod_ente, num_pg = key
            rows.append(
                AggregatedRow(
                    rata_versamento=self._installment,
                    cod_rit=cod_rit,
                    num_zona=num_zona,
                    cod_cspesa=cod_cspesa,
                    capitolo_bil_stato=capitolo_bil_stato,
                    cod_ente=cod_ente,
                    num_pg=num_pg,
                    importo=total_importo,
                )
            )
        return rows

    def write_csv(self, rows: list[AggregatedRow], original_file_name: str) -> Path:
        tmp_file = tempfile.NamedTemporaryFile(
            prefix=f"siniis.{original_file_name}.",
            suffix=".csv",
            delete=False,
            mode="w",
            newline="",
        )
        writer = csv.DictWriter(
            tmp_file,
            fieldnames=[
                "RATA_VERSAMENTO",
                "COD_RIT",
                "NUM_ZONA",
                "COD_CSPESA",
                "CAPITOLO_BIL_STATO",
                "COD_ENTE",
                "NUM_PG",
                "IMPORTO",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "RATA_VERSAMENTO": row.rata_versamento,
                    "COD_RIT": row.cod_rit,
                    "NUM_ZONA": row.num_zona,
                    "COD_CSPESA": row.cod_cspesa,
                    "CAPITOLO_BIL_STATO": row.capitolo_bil_stato,
                    "COD_ENTE": row.cod_ente,
                    "NUM_PG": row.num_pg,
                    "IMPORTO": row.importo,
                }
            )
        tmp_file.flush()
        tmp_file.close()
        logger.info(f"Scritto file SINIIS aggregato in {tmp_file.name}")
        return Path(tmp_file.name)


def aggregate_siniis(file_path: Path, rata: str, resolver: PgResolver, allowed_cod_cspesa: dict[str, int]) -> SiniisAggregationResult:
    aggregator = _SiniisAggregator(str(rata), resolver, allowed_cod_cspesa)
    aggregator.consume_file(file_path)
    rows = aggregator.build_rows()
    output_path = aggregator.write_csv(rows, Path(file_path).name)
    return SiniisAggregationResult(rows=rows, file_path=output_path)


def find_siniis_files(installment: int, flow_type: str, spending_type: str) -> list[Path]:
    """
    Recupera i file SINIIS dalla directory indicata da REMOTE_DIR_FILES.
    """
    folder = REMOTE_DIR_FILES
    if not folder.exists():
        raise FileNotFoundError(f"Cartella non trovata: {folder}")

    matched = [file for file in folder.iterdir() if SINIIS_REGEX.match(file.name)]
    logger.debug(f"Trovati {len(matched)} file SINIIS in {folder}")
    return matched
