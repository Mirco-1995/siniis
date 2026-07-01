# OPI SINIIS

Programma per il caricamento dei dati dal file `siniis_pg` nella tabella `OPI_SINIIS_PG`.

## Utilizzo

```bash
# Con parametri da CLI
opi-siniis --file /path/to/siniis_pg --rata 202607

# Con file properties alternativo
opi-siniis --props /path/to/custom.properties

# Con valori da opi-siniis.properties (default)
opi-siniis
```

## Parametri

| Parametro | Descrizione | Obbligatorio |
|-----------|-------------|--------------|
| `--file`  | Path assoluto del file siniis_pg | No (fallback da properties) |
| `--rata`  | Rata versamento formato YYYYMM | No (fallback da properties) |
| `--props` | Path alternativo del file di properties | No |
| `--verbose` | Abilita logging dettagliato | No |
| `--dry-run` | Esegue solo parsing senza caricare su Oracle | No |

## Configurazione

Copia `opi-siniis.properties.template` in `opi-siniis.properties` e configura:

```ini
[default]
siniis_pg.path = /path/to/siniis_pg
rata_versamento = 202607
```

Le variabili d'ambiente Oracle devono essere configurate:
- `ORACLE_DSN`
- `ORACLE_USER`
- `ORACLE_PASSWORD`
- `ORACLE_OWNER` (default: SPTOWNER)
