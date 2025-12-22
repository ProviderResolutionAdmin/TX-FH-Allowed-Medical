import pandas as pd
import sqlite3
from pathlib import Path

SOURCE_FILE = "allowed_amounts.xlsx"
DB_PATH = Path("data/allowed_amounts.sqlite")

# Load Excel
df = pd.read_excel(SOURCE_FILE)

# Normalize column names
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("%", "th")
)

# Rename explicitly if needed (be honest with your headers)
df = df.rename(columns={
    "geozip": "geozip",
    "code": "code",
    "modifier": "modifier",
    "full_description": "description"
})

# Clean modifier
df["modifier"] = df["modifier"].replace("", None)

# Create DB
conn = sqlite3.connect(DB_PATH)
df.to_sql("allowed_amounts", conn, if_exists="replace", index=False)

# Index for fast lookups
conn.execute("""
CREATE INDEX idx_allowed_lookup
ON allowed_amounts (geozip, code, modifier);
""")

conn.commit()
conn.close()

print("SQLite database created at", DB_PATH)

