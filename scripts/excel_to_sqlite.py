import pandas as pd
import sqlite3
from pathlib import Path

SOURCE_DIR = Path("data/source")
DB_PATH = Path("data/allowed_amounts.sqlite")

REQUIRED_COLS = {"geozip", "code", "product"}

DESC_ALIASES = ["description", "full_description", "procedure_description"]


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("%", "th")
    )
    return df


def normalize_description(df: pd.DataFrame) -> pd.DataFrame:
    for c in DESC_ALIASES:
        if c in df.columns:
            if c != "description":
                df = df.rename(columns={c: "description"})
            return df
    # If no description column exists, keep going but add a blank one (won't crash UI)
    df["description"] = ""
    return df


def normalize_code(df: pd.DataFrame) -> pd.DataFrame:
    df["code"] = (
        df["code"]
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
    )
    return df


def normalize_geozip(df: pd.DataFrame) -> pd.DataFrame:
    # GeoZip must be integer-like; drop rows that cannot be parsed
    df["geozip"] = pd.to_numeric(df["geozip"], errors="coerce")
    df = df.dropna(subset=["geozip"])
    df["geozip"] = df["geozip"].astype(int)
    return df


def normalize_modifier(df: pd.DataFrame) -> pd.DataFrame:
    if "modifier" not in df.columns:
        df["modifier"] = None
        return df

    df["modifier"] = (
        df["modifier"]
        .astype(str)
        .str.strip()
        .replace({"": None, "nan": None, "NaN": None, "None": None})
    )
    return df


def normalize_product(df: pd.DataFrame) -> pd.DataFrame:
    df["product"] = (
        df["product"]
        .astype(str)
        .str.strip()
    )
    return df


def validate_required(df: pd.DataFrame, filename: str):
    missing = REQUIRED_COLS - set(df.columns)
    if missing:
        raise ValueError(f"{filename}: Missing required column(s): {sorted(missing)}")


def build_database():
    excel_files = sorted(SOURCE_DIR.glob("*.xlsx"))
    if not excel_files:
        raise ValueError(f"No .xlsx files found in {SOURCE_DIR}")

    conn = sqlite3.connect(DB_PATH)

    # Replace the table once at start of build (fresh rebuild every run)
    conn.execute("DROP TABLE IF EXISTS allowed_amounts;")
    conn.commit()

    total_rows = 0

    for file_path in excel_files:
        df = pd.read_excel(file_path)

        df = normalize_columns(df)
        validate_required(df, file_path.name)

        df = normalize_description(df)
        df = normalize_code(df)
        df = normalize_geozip(df)
        df = normalize_modifier(df)
        df = normalize_product(df)

        # Traceability: which file did this row come from?
        df["source_file"] = file_path.name

        # Write/append
        df.to_sql("allowed_amounts", conn, if_exists="append", index=False)
        total_rows += len(df)

        print(f"Loaded {len(df)} rows from {file_path.name}")

    # Index for fast lookups
    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_allowed_lookup
    ON allowed_amounts (geozip, code, modifier);
    """)

    # Optional index to filter by product later
    conn.execute("""
    CREATE INDEX IF NOT EXISTS idx_allowed_product
    ON allowed_amounts (product);
    """)

    conn.commit()
    conn.close()

    print(f"SQLite database created at {DB_PATH} with {total_rows} total rows.")


if __name__ == "__main__":
    build_database()
