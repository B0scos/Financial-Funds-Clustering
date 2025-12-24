"""Data validation and manifest generation utilities.

Includes `generate_manifest` which computes basic metadata (rows, columns,
null counts, dtypes) and writes a JSON manifest to the provided directory.
Also includes simple schema validation helpers that raise `CustomException`
on fatal issues.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from src.utils.custom_logger import get_logger
from src.utils.custom_exception import raise_from_exception, CustomException

logger = get_logger(__name__)


def generate_manifest(
    df: pd.DataFrame,
    name: str,
    out_dir: Path,
    sample_n: int = 5,
    required_columns: Optional[list] = None,
    high_null_threshold: float = 0.5,
) -> Tuple[Path, dict]:
    """Generate a manifest JSON describing `df` and write it to `out_dir`.

    Returns (manifest_path, manifest_dict).
    """
    try:
        out_dir.mkdir(parents=True, exist_ok=True)

        manifest = {
            "name": name,
            "created_at": datetime.utcnow().isoformat() + "Z",
            "rows": int(len(df)),
            "columns": int(len(df.columns)),
            "column_names": list(map(str, list(df.columns))),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
            "null_counts": {col: int(df[col].isna().sum()) for col in df.columns},
            "null_fractions": {col: float(df[col].isna().sum() / max(1, len(df))) for col in df.columns},
            "sample_rows": df.head(sample_n).to_dict(orient="records"),
            "issues": [],
        }

        # Required columns check
        if required_columns:
            missing = [c for c in required_columns if c not in df.columns]
            if missing:
                manifest["issues"].append({"type": "missing_columns", "missing": missing})

        # High-null columns
        high_nulls = [c for c, frac in manifest["null_fractions"].items() if frac >= high_null_threshold]
        if high_nulls:
            manifest["issues"].append({"type": "high_null_fraction", "columns": high_nulls})

        manifest_path = out_dir / f"{name}.manifest.json"

        # If there are invalid dates, save a small sample for inspection
        if "report_date" in df.columns:
            invalid_count = int(df["report_date"].isna().sum())
            if invalid_count > 0:
                sample_invalid = df[df["report_date"].isna()].head(100)
                invalid_path = out_dir / f"{name}_invalid_dates_sample.csv"
                sample_invalid.to_csv(invalid_path, index=False)
                manifest["issues"].append({"type": "invalid_dates_sample", "count": invalid_count, "path": str(invalid_path.name)})

        with open(manifest_path, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, ensure_ascii=False, indent=2, default=str)

        logger.info("Wrote manifest for %s to %s", name, manifest_path)
        return manifest_path, manifest

    except Exception as e:
        raise_from_exception("Failed to generate manifest", e)


def validate_required_columns(df: pd.DataFrame, required_columns: list) -> None:
    """Raise CustomException if any required column is missing."""
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise CustomException(f"Missing required columns: {missing}")


# Small helper that combines validation and manifest creation
def validate_and_manifest(
    df: pd.DataFrame,
    name: str,
    out_dir: Path,
    required_columns: Optional[list] = None,
    sample_n: int = 5,
    high_null_threshold: float = 0.5,
) -> Tuple[Path, dict]:
    if required_columns:
        validate_required_columns(df, required_columns)
    return generate_manifest(df, name=name, out_dir=out_dir, sample_n=sample_n, required_columns=required_columns, high_null_threshold=high_null_threshold)
