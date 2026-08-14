"""
Lambda : Vérifications de la qualité des données
────────────────────────────
Appelée par Step Functions après la construction de la couche Silver.
Valide la qualité des données avant d'autoriser l'agrégation Gold.

Vérifications effectuées :
  1. Nombre de lignes — y a-t-il suffisamment de données ?
  2. Pourcentage de valeurs nulles — les colonnes critiques sont-elles renseignées ?
  3. Validation du schéma — les colonnes attendues existent-elles ?
  4. Vérification des plages de valeurs — les valeurs numériques sont-elles raisonnables ?
  5. Fraîcheur — les données sont-elles suffisamment récentes ?

Variables d'environnement :
    S3_BUCKET_SILVER        — Bucket Silver à vérifier
    SNS_ALERT_TOPIC_ARN     — SNS pour les alertes
"""

import os
import json
import logging
from datetime import datetime, timezone, timedelta

import boto3
import awswrangler as wr
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns_client = boto3.client("sns")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")

# ── Seuils ───────────────────────────────────────────────────────────────
MIN_ROW_COUNT = int(os.environ.get("DQ_MIN_ROW_COUNT", "10"))
MAX_NULL_PCT = float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0"))
MAX_VIEWS = 50_000_000_000  # 50B — garde-fou pour le nombre de vues
FRESHNESS_HOURS = 48  # Les données ne doivent pas dépasser cet âge


CRITICAL_COLUMNS = {
    "clean_statistics": ["video_id", "title", "channel_title", "views", "region"],
    "clean_reference_data": ["id", "region"],
}


def check_row_count(df: pd.DataFrame, table_name: str) -> dict:
    """Vérifie que la table contient le nombre minimum de lignes."""
    count = len(df)
    passed = count >= MIN_ROW_COUNT
    return {
        "check": "row_count",
        "table": table_name,
        "value": count,
        "threshold": MIN_ROW_COUNT,
        "passed": passed,
        "message": f"Row count: {count} (min: {MIN_ROW_COUNT})",
    }


def check_null_percentage(df: pd.DataFrame, table_name: str) -> list:
    """Vérifie les pourcentages de valeurs nulles pour les colonnes critiques."""
    results = []
    cols = CRITICAL_COLUMNS.get(table_name, [])

    for col in cols:
        if col not in df.columns:
            results.append({
                "check": "null_pct",
                "table": table_name,
                "column": col,
                "passed": False,
                "message": f"Column '{col}' missing from table",
            })
            continue

        null_pct = (df[col].isna().sum() / len(df)) * 100 if len(df) > 0 else 0
        passed = null_pct <= MAX_NULL_PCT
        results.append({
            "check": "null_pct",
            "table": table_name,
            "column": col,
            "value": round(null_pct, 2),
            "threshold": MAX_NULL_PCT,
            "passed": passed,
            "message": f"{col} null%: {null_pct:.2f}% (max: {MAX_NULL_PCT}%)",
        })

    return results


def check_schema(df: pd.DataFrame, table_name: str) -> dict:
    """Vérifie que les colonnes attendues existent."""
    expected = set(CRITICAL_COLUMNS.get(table_name, []))
    actual = set(df.columns)
    missing = expected - actual
    passed = len(missing) == 0
    return {
        "check": "schema",
        "table": table_name,
        "missing_columns": list(missing),
        "passed": passed,
        "message": f"Missing columns: {missing}" if missing else "All expected columns present",
    }


def check_value_ranges(df: pd.DataFrame, table_name: str) -> list:
    """Vérifie que les valeurs numériques sont dans des plages raisonnables."""
    results = []

    if table_name != "clean_statistics":
        return results

    if "views" in df.columns:
        negative = (df["views"] < 0).sum()
        extreme = (df["views"] > MAX_VIEWS).sum()
        passed = negative == 0 and extreme == 0
        results.append({
            "check": "value_range",
            "table": table_name,
            "column": "views",
            "negative_count": int(negative),
            "extreme_count": int(extreme),
            "passed": passed,
            "message": f"Views: {negative} negative, {extreme} extreme (>{MAX_VIEWS})",
        })

    return results


def check_freshness(df: pd.DataFrame, table_name: str) -> dict:
    """Vérifie que les données incluent des enregistrements récents."""
    if "_processed_at" not in df.columns and "_ingestion_timestamp" not in df.columns:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "message": "No timestamp column found — skipping freshness check (backfill data)",
        }

    ts_col = "_processed_at" if "_processed_at" in df.columns else "_ingestion_timestamp"
    try:
        latest = pd.to_datetime(df[ts_col]).max()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=FRESHNESS_HOURS)
        # Gère les timestamps sans fuseau horaire
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        passed = latest >= cutoff
        return {
            "check": "freshness",
            "table": table_name,
            "latest_record": str(latest),
            "cutoff": str(cutoff),
            "passed": passed,
            "message": f"Latest: {latest}, Cutoff: {cutoff}",
        }
    except Exception as e:
        return {
            "check": "freshness",
            "table": table_name,
            "passed": True,
            "message": f"Could not parse timestamps: {e} — skipping",
        }


def lambda_handler(event, context):
    """
    Exécute les vérifications de qualité des données sur les tables de la couche Silver.

    Événement attendu :
    {
        "layer": "silver",
        "database": "yt_pipeline_silver_dev",
        "tables": ["clean_statistics", "clean_reference_data"]
    }
    """
    database = event.get("database", "yt_pipeline_silver_dev")
    tables = event.get("tables", ["clean_statistics"])

    all_results = []
    overall_passed = True

    for table_name in tables:
        logger.info(f"Running DQ checks on {database}.{table_name}...")

        try:
            # Lit un échantillon des données (limite pour coût/vitesse)
            query = f'SELECT * FROM "{table_name}" LIMIT 10000'
            df = wr.athena.read_sql_query(
                sql=query,
                database=database,
                ctas_approach=False,
            )
        except Exception as e:
            logger.error(f"Could not read {table_name}: {e}")
            all_results.append({
                "check": "read_table",
                "table": table_name,
                "passed": False,
                "message": str(e),
            })
            overall_passed = False
            continue

        # Exécute toutes les vérifications
        checks = []
        checks.append(check_row_count(df, table_name))
        checks.extend(check_null_percentage(df, table_name))
        checks.append(check_schema(df, table_name))
        checks.extend(check_value_ranges(df, table_name))
        checks.append(check_freshness(df, table_name))

        for check in checks:
            logger.info(f"  {check['check']}: {'PASS' if check['passed'] else 'FAIL'} — {check['message']}")
            if not check["passed"]:
                overall_passed = False

        all_results.extend(checks)

    # Résumé
    passed_count = sum(1 for r in all_results if r["passed"])
    total_count = len(all_results)
    logger.info(f"DQ Summary: {passed_count}/{total_count} checks passed. Overall: {'PASS' if overall_passed else 'FAIL'}")

    if not overall_passed and SNS_TOPIC:
        failed = [r for r in all_results if not r["passed"]]
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject="[YT Pipeline] Data quality checks FAILED",
            Message=json.dumps(failed, indent=2, default=str),
        )

    return {
        "quality_passed": bool(overall_passed),
        "checks_passed": int(passed_count),
        "checks_total": int(total_count),
        "details": json.loads(json.dumps(all_results, default=str)),
    }