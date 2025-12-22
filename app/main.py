from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from pathlib import Path
import sqlite3
from datetime import datetime
from typing import List

app = FastAPI(title="TX FH Allowed Medical Lookup")

DB_PATH = Path("data/allowed_amounts.sqlite")
UI_PATH = Path("frontend/index.html")


# -----------------------
# Database helpers
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


def ensure_log_table(conn):
    """
    Create usage log table if it does not exist.
    Code is stored as TEXT to match lookup behavior.
    """
    conn.execute("""
    CREATE TABLE IF NOT EXISTS lookup_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lookup_time TEXT NOT NULL,
        geozip INTEGER NOT NULL,
        code TEXT NOT NULL,
        modifier TEXT,
        match_type TEXT,
        success INTEGER NOT NULL
    )
    """)
    conn.commit()


def log_lookup(conn, geozip, code, modifier, match_type, success):
    conn.execute("""
    INSERT INTO lookup_log (
        lookup_time,
        geozip,
        code,
        modifier,
        match_type,
        success
    )
    VALUES (?, ?, ?, ?, ?, ?)
    """, (
        datetime.utcnow().isoformat(),
        geozip,
        code,
        modifier,
        match_type,
        success
    ))
    conn.commit()


# -----------------------
# UI
# -----------------------
@app.get("/", response_class=HTMLResponse)
def serve_ui():
    if not UI_PATH.exists():
        raise HTTPException(status_code=500, detail="UI file not found")
    return UI_PATH.read_text()


# -----------------------
# Lookup API (corrected + hardened)
# -----------------------
from typing import List

@app.get("/lookup")
def lookup(
    geozip: int = Query(...),
    code: List[str] = Query(..., description="One or more procedure codes"),
    modifier: str | None = Query(default=None)
):
    conn = get_connection()
    ensure_log_table(conn)

    modifier = modifier.strip() if modifier else None
    results = []

    try:
        for c in code:
            c = c.strip()
            row = None
            match_type = None

            # 1. Modifier-specific lookup
            if modifier:
                row = conn.execute(
                    """
                    SELECT *
                    FROM allowed_amounts
                    WHERE geozip = ?
                      AND code = ?
                      AND modifier = ?
                    """,
                    (geozip, c, modifier)
                ).fetchone()

                if row:
                    match_type = "Modifier-specific rate"

            # 2. Base rate lookup
            if not row:
                row = conn.execute(
                    """
                    SELECT *
                    FROM allowed_amounts
                    WHERE geozip = ?
                      AND code = ?
                      AND (modifier IS NULL OR modifier = '')
                    """,
                    (geozip, c)
                ).fetchone()

                if row:
                    match_type = (
                        "Base rate (no modifier)"
                        if not modifier
                        else "Base rate (modifier not on file)"
                    )

            if row:
                result = dict(row)
                result["match_type"] = match_type
                results.append(result)
                log_lookup(conn, geozip, c, modifier, match_type, 1)
            else:
                # Return a visible "no match" row for this code
                results.append({
                    "description": f"No match found for code {c}",
                    "match_type": "No match",
                    "50th": "",
                    "60th": "",
                    "70th": "",
                    "75th": "",
                    "80th": "",
                    "85th": "",
                    "90th": "",
                    "95th": ""
                })
                log_lookup(conn, geozip, c, modifier, "No match found", 0)

        return results  # <-- ALWAYS a list

    finally:
        conn.close()
