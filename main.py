import os
import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Request
from typing import Optional

app = FastAPI(
    title="API Bazy Produktowej",
    description="API do interaktywnego przeszukiwania indeksu produktowego.",
    version="3.0.0-postgres",
)

DATABASE_URL = os.environ.get("DATABASE_URL") # To jest pobierane z ustawień Render

def get_db_connection():
    """Funkcja łączy się z bazą danych PostgreSQL."""
    conn = psycopg2.connect(DATABASE_URL)
    return conn

@app.get("/")
def read_root():
    return {"message": "Witaj w API Bazy Produktowej! Gotowy do działania."}

@app.get("/produkt/{symbol}")
def pobierz_produkt_po_symbolu(symbol: str):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute("SELECT * FROM produkty WHERE \"Symbol\" = %s", (symbol,))
    product = cursor.fetchone()
    cursor.close()
    conn.close()
    if product is None:
        raise HTTPException(status_code=404, detail="Produkt o podanym symbolu nie został znaleziony.")
    return dict(product)

@app.get("/wyszukaj/")
def wyszukaj_produkty(request: Request, fields: Optional[str] = None):
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'produkty'")
    available_columns = {row[0] for row in cursor.fetchall()}

    select_fields = "*"
    if fields:
        requested_fields = [field.strip() for field in fields.split(',')]
        valid_fields = [field for field in requested_fields if field in available_columns]
        if valid_fields:
            select_fields = ", ".join(f'"{field}"' for field in valid_fields)
        else:
            select_fields = '"Symbol"'

    query_params = dict(request.query_params)
    if 'fields' in query_params:
        del query_params['fields']

    filters = {k: v for k, v in query_params.items() if k in available_columns}

    base_query = f"SELECT {select_fields} FROM produkty"
    values = []

    if filters:
        where_clauses = []
        for key, value in filters.items():
            where_clauses.append(f'"{key}" ILIKE %s') # ILIKE to wersja "bez wielkości liter" dla PostgreSQL
            values.append(value)
        base_query += " WHERE " + " AND ".join(where_clauses)

    cursor.execute(base_query, tuple(values))
    results = cursor.fetchall()
    cursor.close()
    conn.close()
    return [dict(row) for row in results]
