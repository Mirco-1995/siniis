import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from opi_siniis.core import OracleSiniisLoader, parse_cobol_signed, parse_file, parse_line


def make_record(*, tipo_zona="V", num_zona="0001", importo="00001234") -> bytes:
    record = bytearray(b" " * 122)

    def put(start: int, value: str) -> None:
        encoded = value.encode("latin-1")
        record[start - 1:start - 1 + len(encoded)] = encoded

    values = {
        1: "E", 2: "3", 3: "A07", 6: tipo_zona, 7: num_zona,
        11: "1037", 15: "2646", 19: "00107043", 27: "L001",
        31: "520", 34: importo, 42: "20260630", 50: "00000344",
        58: "E", 59: "A", 67: "M", 68: "1", 69: "0",
        119: "01", 121: "02",
    }
    for start, value in values.items():
        put(start, value)
    return bytes(record)


class ParseCoreTests(unittest.TestCase):
    def test_fixed_field_mapping(self):
        result = parse_line(make_record(), 202606, 1)

        self.assertTrue(result.success)
        self.assertEqual(result.record.rata_versamento, 202606)
        self.assertEqual(result.record.cod_rit, "A07")
        self.assertEqual(result.record.num_zona, "0001")
        self.assertEqual(result.record.cod_cspesa, 1037)
        self.assertEqual(result.record.provincia, 520)
        self.assertEqual(result.record.importo, 1234)
        self.assertEqual(result.record.num_pg, "02")

    def test_local_zone_uses_positions_27_to_30(self):
        result = parse_line(make_record(tipo_zona="L"), 202606, 1)

        self.assertTrue(result.success)
        self.assertEqual(result.record.num_zona, "L001")

    def test_negative_cobol_overpunch(self):
        self.assertEqual(parse_cobol_signed("0001231M"), -12314)

    def test_file_without_line_separators(self):
        self._assert_file_results(make_record() + make_record(), [True, True])

    def test_file_with_crlf_and_invalid_middle_record(self):
        data = b"\r\n".join((make_record(), b"invalid", make_record()))
        self._assert_file_results(data, [True, False, True])

    def _assert_file_results(self, data: bytes, expected: list[bool]):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "siniis_pg"
            path.write_bytes(data)
            results = list(parse_file(path, 202606))

        self.assertEqual([result.success for result in results], expected)
        self.assertEqual(
            [result.line_number for result in results],
            list(range(1, len(expected) + 1)),
        )


class FakeCursor:
    def __init__(self):
        self.executions = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, parameters=None):
        self.executions.append((" ".join(statement.split()), parameters))


class FakeConnection:
    def __init__(self):
        self.cursor_instance = FakeCursor()
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.committed = True


class OracleLoaderTests(unittest.TestCase):
    def test_load_rebuilds_owned_indexes_and_inserts_record(self):
        parsed = parse_line(make_record(), 202606, 1)
        connection = FakeConnection()
        loader = OracleSiniisLoader(
            dsn="db.example/pdb",
            user="user",
            password="password",
            owner="SPTOWNER",
        )

        with patch.object(loader, "_get_connection", return_value=connection):
            result = loader.load_records([parsed.record], 202606)

        executions = connection.cursor_instance.executions
        statements = [item[0] for item in executions]
        self.assertIn(
            "ALTER TABLE SPTOWNER.OPI_SINIIS_PG TRUNCATE PARTITION P_202606",
            statements[0],
        )
        for index_number, statement in enumerate(statements[1:5], start=1):
            self.assertIn(f"SPTOWNER.IDX_SINIIS_PG_0{index_number}", statement)
            self.assertIn("REBUILD PARTITION P_202606", statement)
        self.assertIn("INSERT INTO SPTOWNER.OPI_SINIIS_PG", statements[5])
        self.assertEqual(len(executions[5][1]), 20)
        self.assertTrue(connection.committed)
        self.assertEqual(result.loaded, 1)
        self.assertEqual(result.skipped, 0)


if __name__ == "__main__":
    unittest.main()
