from __future__ import annotations

import os
from dataclasses import dataclass

from loguru import logger

try:
    import oracledb
except ImportError:
    oracledb = None

from opi_siniis.constants import (
    COBOL_NUMERIC_MAP,
    ORACLE_DSN,
    ORACLE_HOME,
    ORACLE_OWNER,
    ORACLE_PASSWORD,
    ORACLE_USER,
)

MIN_RECORD_LENGTH = 132
DB_INSERT_BATCH_SIZE = 10_000
DB_PROGRESS_STEP = 1_000_000


@dataclass
class SiniisRecord:
    rata_versamento: int
    tipo_rit_raggrup: str | None
    mod_pag: str | None
    cod_rit: str
    tipo_zona: str | None
    num_zona: str | None
    cod_cspesa: int
    capitolo_bil_stato: int
    iscrizione: int
    provincia: int
    importo: int
    data_trattamento: int
    num_ordine: int
    provenienza: str | None
    tipo_ritenuta: str | None
    sesso: str | None
    part_time: str | None
    lsu: str | None
    progr_emissione: int | None
    num_pg: str | None
    tipo_pg: str | None


@dataclass
class ParseResult:
    success: bool
    record: SiniisRecord | None = None
    error: str | None = None
    line_number: int = 0


@dataclass
class LoadResult:
    total_lines: int = 0
    loaded: int = 0
    skipped: int = 0
    errors: list[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


def parse_cobol_signed(raw_value: str) -> int:
    raw_value = raw_value.strip()
    if not raw_value:
        return 0

    last_char = raw_value[-1]

    if last_char in COBOL_NUMERIC_MAP:
        mapped_digit = COBOL_NUMERIC_MAP[last_char]
        numeric = f"{raw_value[:-1]}{mapped_digit}"
    else:
        numeric = raw_value

    if not numeric.isdigit():
        raise ValueError(f"Valore importo non riconosciuto: {raw_value}")

    if last_char in COBOL_NUMERIC_MAP:
        return int(numeric) * -1
    return int(numeric)


def _decode_field(raw: bytes, start: int, length: int) -> str:
    start_idx = start - 1
    end_idx = start_idx + length
    return raw[start_idx:end_idx].decode("latin-1", errors="ignore").strip()


def _parse_int_field(value: str) -> int | None:
    value = value.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_line(line: bytes, rata_versamento: int, line_number: int) -> ParseResult:
    try:
        if len(line.strip()) == 0:
            return ParseResult(success=False, error="Riga vuota", line_number=line_number)

        tipo_rit_raggrup = _decode_field(line, 1, 1) or None
        mod_pag = _decode_field(line, 2, 1) or None
        cod_rit = _decode_field(line, 3, 3)
        tipo_zona = _decode_field(line, 6, 1) or None

        if tipo_zona == "L":
            num_zona = _decode_field(line, 27, 4) or None
        else:
            num_zona = _decode_field(line, 7, 4) or None

        cod_cspesa_str = _decode_field(line, 11, 4)
        capitolo_bil_stato_str = _decode_field(line, 15, 4)
        iscrizione_str = _decode_field(line, 19, 8)
        provincia_str = _decode_field(line, 31, 3)
        importo_raw = _decode_field(line, 34, 8)
        data_trattamento_str = _decode_field(line, 42, 8)
        num_ordine_str = _decode_field(line, 50, 8)
        provenienza = _decode_field(line, 58, 1) or None
        tipo_ritenuta = _decode_field(line, 59, 1) or None
        sesso = _decode_field(line, 67, 1) or None
        part_time = _decode_field(line, 68, 1) or None
        lsu = _decode_field(line, 69, 1) or None
        progr_emissione_str = _decode_field(line, 119, 2)
        num_pg = _decode_field(line, 131, 2) or None
        tipo_pg_length = len(line) - 132
        tipo_pg = _decode_field(line, 133, tipo_pg_length) or None

        cod_cspesa = _parse_int_field(cod_cspesa_str)
        capitolo_bil_stato = _parse_int_field(capitolo_bil_stato_str)
        iscrizione = _parse_int_field(iscrizione_str)
        provincia = _parse_int_field(provincia_str)
        data_trattamento = _parse_int_field(data_trattamento_str)
        num_ordine = _parse_int_field(num_ordine_str)
        if num_ordine is None:
            num_ordine = 0
        progr_emissione = _parse_int_field(progr_emissione_str)

        try:
            importo = parse_cobol_signed(importo_raw)
        except ValueError as e:
            return ParseResult(
                success=False,
                error=f"Errore parsing importo: {e}",
                line_number=line_number,
            )

        errors = []
        if not cod_rit:
            errors.append("COD_RIT e' vuoto")
        if cod_cspesa is None:
            errors.append("COD_CSPESA non valido")
        if capitolo_bil_stato is None:
            errors.append("CAPITOLO_BIL_STATO non valido")
        if iscrizione is None:
            errors.append("ISCRIZIONE non valido")
        if provincia is None:
            errors.append("PROVINCIA non valido")
        if data_trattamento is None:
            errors.append("DATA_TRATTAMENTO non valido")

        if errors:
            return ParseResult(
                success=False,
                error="; ".join(errors),
                line_number=line_number,
            )

        record = SiniisRecord(
            rata_versamento=rata_versamento,
            tipo_rit_raggrup=tipo_rit_raggrup,
            mod_pag=mod_pag,
            cod_rit=cod_rit,
            tipo_zona=tipo_zona,
            num_zona=num_zona,
            cod_cspesa=cod_cspesa,
            capitolo_bil_stato=capitolo_bil_stato,
            iscrizione=iscrizione,
            provincia=provincia,
            importo=importo,
            data_trattamento=data_trattamento,
            num_ordine=num_ordine,
            provenienza=provenienza,
            tipo_ritenuta=tipo_ritenuta,
            sesso=sesso,
            part_time=part_time,
            lsu=lsu,
            progr_emissione=progr_emissione,
            num_pg=num_pg,
            tipo_pg=tipo_pg,
        )

        return ParseResult(success=True, record=record, line_number=line_number)

    except Exception as e:
        return ParseResult(
            success=False,
            error=f"Errore parsing: {e}",
            line_number=line_number,
        )


class OracleSiniisLoader:

    def __init__(
        self,
        dsn: str | None = None,
        user: str | None = None,
        password: str | None = None,
        owner: str | None = None,
    ):
        if oracledb is None:
            raise ImportError("oracledb non installato")

        self._dsn = dsn or ORACLE_DSN
        self._user = user or ORACLE_USER
        self._password = password or ORACLE_PASSWORD
        self._owner = owner or ORACLE_OWNER

        if not all((self._dsn, self._user, self._password)):
            raise EnvironmentError(
                "Variabili d'ambiente non definite: ORACLE_DSN, ORACLE_USER, ORACLE_PASSWORD"
            )

        self._table_name = f"{self._owner}.OPI_SINIIS_PG"

    def _init_oracle_client(self):
        if hasattr(oracledb, "is_thin_mode") and not oracledb.is_thin_mode():
            return

        oracle_home = (ORACLE_HOME or "").strip()
        if not oracle_home:
            raise EnvironmentError("Variabile d'ambiente non definita: ORACLE_HOME")

        oracledb.init_oracle_client(lib_dir=os.path.join(oracle_home, "lib"))

    def _get_connection(self):
        self._init_oracle_client()
        return oracledb.connect(
            user=self._user,
            password=self._password,
            dsn=self._dsn,
        )

    def _record_to_params(self, rec: SiniisRecord) -> tuple:
        return (
            rec.rata_versamento,
            rec.tipo_rit_raggrup,
            rec.mod_pag,
            rec.cod_rit,
            rec.tipo_zona,
            rec.num_zona,
            rec.cod_cspesa,
            rec.capitolo_bil_stato,
            rec.iscrizione,
            rec.provincia,
            rec.importo,
            rec.data_trattamento,
            rec.num_ordine,
            rec.provenienza,
            rec.tipo_ritenuta,
            rec.sesso,
            rec.part_time,
            rec.lsu,
            rec.progr_emissione,
            rec.num_pg,
            rec.tipo_pg,
        )

    def _load_failed_batch_one_by_one(
        self,
        cur,
        insert_sql: str,
        records: list[SiniisRecord],
        record_number_offset: int,
        result: LoadResult,
    ):
        for rec_index, rec in enumerate(records, start=record_number_offset + 1):
            try:
                cur.execute(insert_sql, self._record_to_params(rec))
                result.loaded += 1
            except oracledb.DatabaseError as e:
                error_obj, = e.args
                if error_obj.code == 14400:
                    logger.critical(
                        f"ORA-14400: Partizione non trovata per rata {rec.rata_versamento}. "
                        "La partizione mensile e' di competenza del DBA."
                    )
                    raise

                result.skipped += 1
                result.errors.append(f"Record {rec_index}: {e}")
                logger.warning(f"Scartato record {rec_index}: {e}")

    def load_records(
        self,
        records: list[SiniisRecord],
        rata: int,
        truncate_partition: bool = True,
        record_number_offset: int = 0,
    ) -> LoadResult:
        result = LoadResult(total_lines=len(records))

        if not records:
            logger.info("Nessun record da caricare")
            return result

        partition_name = f"P_{rata}"

        truncate_sql = f"""
            ALTER TABLE {self._table_name} TRUNCATE PARTITION {partition_name}
        """

        rebuild_indices = [
            f"ALTER INDEX IDX_SINIIS_PG_01 REBUILD PARTITION {partition_name}",
            f"ALTER INDEX IDX_SINIIS_PG_02 REBUILD PARTITION {partition_name}",
            f"ALTER INDEX IDX_SINIIS_PG_03 REBUILD PARTITION {partition_name}",
            f"ALTER INDEX IDX_SINIIS_PG_04 REBUILD PARTITION {partition_name}",
        ]

        insert_sql = f"""
            INSERT INTO {self._table_name} (
                OPI_RATA_VERSAMENTO,
                OPI_TIPO_RIT_RAGGRUP,
                OPI_MOD_PAG,
                OPI_COD_RIT,
                OPI_TIPO_ZONA,
                OPI_NUM_ZONA,
                OPI_COD_CSPESA,
                OPI_CAPITOLO_BIL_STATO,
                OPI_ISCRIZIONE,
                OPI_PROVINCIA,
                OPI_IMPORTO,
                OPI_DATA_TRATTAMENTO,
                OPI_NUM_ORDINE,
                OPI_PROVENIENZA,
                OPI_TIPO_RITENUTA,
                OPI_SESSO,
                OPI_PART_TIME,
                OPI_LSU,
                OPI_PROGR_EMISSIONE,
                OPI_NUM_PG,
                OPI_TIPO_PG
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8, :9, :10,
                :11, :12, :13, :14, :15, :16, :17, :18, :19, :20,
                :21
            )
        """

        try:
            with self._get_connection() as conn:
                with conn.cursor() as cur:
                    if truncate_partition:
                        cur.execute(truncate_sql)
                        logger.info(f"TRUNCATE PARTITION {partition_name} completata")

                        for idx_sql in rebuild_indices:
                            cur.execute(idx_sql)
                        logger.info(f"REBUILD indici per partizione {partition_name} completato")

                    next_progress = DB_PROGRESS_STEP

                    for batch_start in range(0, len(records), DB_INSERT_BATCH_SIZE):
                        batch = records[batch_start:batch_start + DB_INSERT_BATCH_SIZE]
                        batch_params = [self._record_to_params(rec) for rec in batch]
                        first_record_number = record_number_offset + batch_start

                        try:
                            cur.executemany(insert_sql, batch_params)
                            result.loaded += len(batch)
                        except oracledb.DatabaseError as e:
                            error_obj, = e.args
                            if error_obj.code == 14400:
                                logger.critical(
                                    f"ORA-14400: Partizione non trovata per rata {rata}. "
                                    "La partizione mensile e' di competenza del DBA."
                                )
                                raise

                            logger.warning(
                                "Errore su un batch DB: riprovo i record del batch uno alla volta"
                            )
                            self._load_failed_batch_one_by_one(
                                cur=cur,
                                insert_sql=insert_sql,
                                records=batch,
                                record_number_offset=first_record_number,
                                result=result,
                            )

                        processed = batch_start + len(batch)
                        if processed >= next_progress or processed == result.total_lines:
                            logger.info(
                                f"Inserimento blocco: {processed}/{result.total_lines} record elaborati"
                            )
                            while next_progress <= processed:
                                next_progress += DB_PROGRESS_STEP

                conn.commit()

        except oracledb.DatabaseError as e:
            error_obj, = e.args
            if error_obj.code == 14400:
                raise RuntimeError(
                    f"ORA-14400: la partizione per rata {rata} non esiste. "
                    "La partizione è di competenza del DBA. Esecuzione interrotta."
                )
            raise

        logger.info(f"Caricati {result.loaded}/{result.total_lines} record, scartati {result.skipped}")
        return result
