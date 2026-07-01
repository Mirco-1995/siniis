# OPI SINIIS

Programma per il caricamento dei dati dal file `siniis_pg` nella tabella `OPI_SINIIS_PG`.

## Utilizzo

```bash
# Con path file da CLI
opi-siniis --file /path/to/siniis_pg --rata 202607

# Con path file da .env (default)
opi-siniis --rata 202607
```

## Parametri

| Parametro | Descrizione | Obbligatorio |
|-----------|-------------|--------------|
| `--file`  | Path assoluto del file siniis_pg | No (fallback da .env) |
| `--rata`  | Rata versamento formato YYYYMM | Sì |

## Configurazione

Copia `.env.template` in `.env` e configura:

- `SINIIS_PG_FILE_PATH`: path di default del file siniis_pg
- `ORACLE_DSN`: DSN Oracle
- `ORACLE_USER`: utente Oracle
- `ORACLE_PASSWORD`: password Oracle
- `ORACLE_OWNER`: owner tabella (default: SPTOWNER)
