# Importujemy potrzebne narzędzia
import sqlite3
from fastapi import FastAPI, HTTPException, Request
from typing import List, Optional

# Tworzymy instancję naszej aplikacji API
app = FastAPI(
    title="API Bazy Produktowej",
    description="API do interaktywnego przeszukiwania indeksu produktowego.",
    version="2.0.0", # Wersja dla Render.com
)

# Definiujemy nazwę pliku z naszą bazą danych (plik będzie w tym samym folderze)
DATABASE_NAME = "indeks_produktowy.sqlite"

def get_db_connection():
    """Funkcja pomocnicza do łączenia się z bazą danych."""
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/")
def read_root():
    return {"message": "Witaj w API Bazy Produktowej! Gotowy do działania."}

@app.get("/produkt/{symbol}")
def pobierz_produkt_po_symbolu(symbol: str):
    conn = get_db_connection()
    product = conn.execute("SELECT * FROM produkty WHERE Symbol = ?", (symbol,)).fetchone()
    conn.close()
    if product is None:
        raise HTTPException(status_code=404, detail="Produkt o podanym symbolu nie został znaleziony.")
    return dict(product)

@app.get("/wyszukaj/")
def wyszukaj_produkty(request: Request, fields: Optional[str] = None):
    """
    Przeszukuje bazę produktów na podstawie dowolnych parametrów z zapytania.
    Wyszukiwanie jest niewrażliwe na wielkość liter.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(produkty)")
    available_columns = {row[1] for row in cursor.fetchall()}

    select_fields = "*"
    if fields:
        requested_fields = [field.strip() for field in fields.split(',')]
        valid_fields = [field for field in requested_fields if field in available_columns]
        if valid_fields:
            select_fields = ", ".join(f'"{field}"' for field in valid_fields)
        else:
            select_fields = "Symbol"

    query_params = dict(request.query_params)
    if 'fields' in query_params:
        del query_params['fields']
        
    filters = {k: v for k, v in query_params.items() if k in available_columns}
    where_clauses = []
    values = []

    for key, value in filters.items():
        where_clauses.append(f'"{key}" = ? COLLATE NOCASE')
        values.append(value)
        
    base_query = f"SELECT {select_fields} FROM produkty"
    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)
    
    results = cursor.execute(base_query, tuple(values)).fetchall()
    conn.close()

    return [dict(row) for row in results]