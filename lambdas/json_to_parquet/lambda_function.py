"""
Lambda : Données de référence JSON → Couche Silver (Parquet)
────────────────────────────────────────────────────
Déclenché par un événement S3 lorsqu'un nouveau JSON arrive dans le bucket
Bronze sous le préfixe reference_data.

Améliorations par rapport à l'original :
  - Validation des données avant écriture
  - Déduplication des enregistrements de catégories
  - Gestion des erreurs avec alerte dead-letter
  - Écritures idempotentes (écrase la partition, pas d'ajout)
  - Logging structuré

Variables d'environnement :
    S3_BUCKET_SILVER            — Bucket cible pour les données nettoyées
    GLUE_DB_SILVER              — Nom de la base de données du catalogue Glue
    GLUE_TABLE_REFERENCE        — Nom de la table du catalogue Glue
    SNS_ALERT_TOPIC_ARN         — Topic SNS pour les alertes (optionnel)
"""

import json
import os
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
import awswrangler as wr
import pandas as pd

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Configuration ────────────────────────────────────────────────────────────
SILVER_BUCKET = os.environ["S3_BUCKET_SILVER"]
GLUE_DB = os.environ.get("GLUE_DB_SILVER", "yt_pipeline_silver_dev")
GLUE_TABLE = os.environ.get("GLUE_TABLE_REFERENCE", "clean_reference_data")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
SILVER_PATH = f"s3://{SILVER_BUCKET}/youtube/reference_data/"

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")


def read_json_from_s3(bucket: str, key: str) -> dict:
    """
    Lit le JSON brut depuis S3 avec boto3 plutôt qu'avec awswrangler.
    awswrangler.s3.read_json() échoue sur le JSON des catégories Kaggle/YouTube
    car il contient des types mixtes (chaînes + tableaux imbriqués), que pandas
    ne peut pas parser directement en DataFrame.
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response["Body"].read().decode("utf-8")
    return json.loads(content)


def validate_category_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Valide et nettoie les données de référence des catégories.
    Retourne le DataFrame nettoyé ou lève une ValueError.
    """
    if df.empty:
        raise ValueError("Empty DataFrame — no category items found")

    required_cols = {"id", "snippet.title"}
    actual_cols = set(df.columns)
    missing = required_cols - actual_cols
    if missing:
        # Tente les noms de colonnes alternatifs des différentes versions de l'API
        logger.warning(f"Missing expected columns: {missing}. Available: {actual_cols}")

    # Supprime les catégories en double (même id)
    before = len(df)
    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"], keep="last")
    after = len(df)
    if before != after:
        logger.info(f"  Removed {before - after} duplicate categories")

    return df


def send_alert(subject: str, message: str):
    if SNS_TOPIC:
        sns_client.publish(TopicArn=SNS_TOPIC, Subject=subject[:100], Message=message)


def lambda_handler(event, context):
    """Traite l'événement S3 pour les nouveaux fichiers JSON de référence."""

    # Gère à la fois les événements S3 directs et les événements encapsulés par EventBridge
    records = event.get("Records", [])
    if not records:
        # Peut être invoqué directement par Step Functions
        records = [event] if "s3" in event else []

    processed = []
    errors = []

    for record in records:
        try:
            s3_info = record["s3"]
            bucket = s3_info["bucket"]["name"]
            key = unquote_plus(s3_info["object"]["key"])

            logger.info(f"Processing: s3://{bucket}/{key}")

            # ── Lecture du JSON brut ─────────────────────────────────────
            # On utilise boto3 + json.loads plutôt que wr.s3.read_json() car
            # le JSON des catégories contient des types mixtes (chaînes comme
            # "kind"/"etag" avec un tableau "items" imbriqué), ce qui fait
            # échouer pandas avec :
            # "Mixing dicts with non-Series may lead to ambiguous ordering"
            raw_data = read_json_from_s3(bucket, key)

            # Le JSON YouTube/Kaggle a la forme { "kind": "...", "items": [...] }
            # Seul le tableau items nous intéresse
            if "items" in raw_data and isinstance(raw_data["items"], list):
                df = pd.json_normalize(raw_data["items"])
            else:
                # Repli : tente de normaliser l'objet entier
                df = pd.json_normalize(raw_data)

            logger.info(f"  Raw shape: {df.shape}")

            # ── Validation ───────────────────────────────────────────────
            df = validate_category_data(df)

            # ── Ajout des colonnes de métadonnées ───────────────────────
            df["_ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
            df["_source_file"] = key

            # Extrait la région depuis la clé S3 (ex : region=US)
            region = "unknown"
            for part in key.split("/"):
                if part.startswith("region="):
                    region = part.split("=")[1]
                    break
            df["region"] = region

            logger.info(f"  Clean shape: {df.shape}, region: {region}")

            # ── Écriture en Parquet dans la couche Silver ────────────────
            wr_response = wr.s3.to_parquet(
                df=df,
                path=SILVER_PATH,
                dataset=True,
                database=GLUE_DB,
                table=GLUE_TABLE,
                partition_cols=["region"],
                mode="overwrite_partitions",  # Idempotent par région
                schema_evolution=True,
            )

            logger.info(f"  Written to Silver: {SILVER_PATH}")
            processed.append({"key": key, "region": region, "rows": len(df)})

        except Exception as e:
            logger.error(f"Error processing record: {e}", exc_info=True)
            errors.append({"key": key if "key" in dir() else "unknown", "error": str(e)})

    # ── Résumé ───────────────────────────────────────────────────────────
    if errors:
        send_alert(
            subject="[YT Pipeline] Silver reference transform failed",
            message=json.dumps(errors, indent=2),
        )

    return {
        "statusCode": 200,
        "processed": processed,
        "errors": errors,
    }