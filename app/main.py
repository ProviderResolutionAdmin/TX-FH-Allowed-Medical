from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pathlib import Path
import sqlite3

app = FastAPI(title="TX FH Allowed Medical Lookup")

DB_PATH = Path("data/allowed_amounts.sqlite")
UI_PATH = Path("frontend/index.html")


# -----------------------
# Database helper
# -----------------------
def get_connection():
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=500,
            detail="Allowed amounts database not found. Data build may not have run."
        )
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# -----------------------
# UI
# -----------------------
@app.get("/", response_class=HTMLResponse)
def serve_ui():
    if not UI_PATH.exists():
        raise HTTPException(status_code=500, detail="UI file not found")
    return UI_PATH.read_text()


# -----------------------
# Lookup API with validation + fallback
# -----------------------
@app.get("/lookup")
def lookup(
    geozip: int = Query(..., description="Geographic ZIP"),
    code: int = Query(..., description="Procedure code"),
    modifier: int | None = Query(default=None)
):
    conn = get_connection()

    try:
        # Case 1: Modifier provided — try modifier-specific row
        if modifier is not None:
            row = conn.execute(
                """
                SELECT *
                FROM allowed_amounts
                WHERE geozip = ?
                  AND code = ?
                  AND modifier = ?
                """,
                (geozip, code, modifier)
            ).fetchone()

            if row:
                result = dict(row)
                result["match_type"] = "Modifier-specific rate"
                return result

        # Case 2: Base rate (no modifier) — this is the NORMAL path
        row = conn.execute(
            """
            SELECT *
            FROM allowed_amounts
            WHERE geozip = ?
              AND code = ?
              AND modifier IS NULL
            """,
            (geozip, code)
        ).fetchone()

        if row:
            result = dict(row)
            result["match_type"] = (
                "Base rate (no modifier)"
                if modifier is None
                else "Base rate (modifier not on file)"
            )
            return result

        # Case 3: Nothing found
        raise HTTPException(
            status_code=404,
            detail=(
                f"No allowed amount found for GeoZip {geozip} "
                f"and Procedure Code {code}"
                + (f" with Modifier {modifier}" if modifier else "")
            )
        )

    finally:
        conn.close()
