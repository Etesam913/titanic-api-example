from pprint import pprint
import sqlite3
import csv
from fastapi import FastAPI, UploadFile, HTTPException
from io import StringIO

app = FastAPI()


@app.post("/uploadcsv")
async def load_data(file: UploadFile):
    if file.content_type != "text/csv":
        raise HTTPException(status_code=422, detail="The uploaded file must be a csv!")
    file_contents = await file.read()
    buffer = StringIO(file_contents.decode("utf-8"))
    csv_reader = csv.DictReader(buffer)
    columns = set()

    conn = sqlite3.connect("titanic.db")

    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS titanic_data")

    cursor.execute(
        """
        CREATE TABLE titanic_data(
          id INTEGER PRIMARY KEY,
          survived INTEGER,
          name TEXT,
          sex TEXT,
          age INTEGER,
          ticket INTEGER,
          cabin TEXT
        );
      """
    )
    csv_to_db_map = {
        "PassengerId": "id",
        "Survived": "survived",
        "Name": "name",
        "Sex": "sex",
        "Age": "age",
        "Ticket": "ticket",
        "Cabin": "cabin",
    }
    items_to_insert = []
    for row in csv_reader:
        item = []
        for key, val in row.items():
            if key in csv_to_db_map:
                item.append(val)
        items_to_insert.append(tuple(item))

    cursor.executemany(
        """
        INSERT INTO titanic_data (id, survived, name, sex, age, ticket, cabin)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        items_to_insert,
    )
    cursor.execute(
        """
        SELECT * FROM titanic_data
        """
    )
    rows = cursor.fetchall()

    for row in rows:
        print(row)

    conn.commit()
    conn.close()
    return {"type": file.content_type, "data": {}}
