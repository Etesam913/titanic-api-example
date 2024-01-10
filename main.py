from pprint import pprint
import sqlite3
import csv
from typing import Union
from fastapi import FastAPI, UploadFile, HTTPException
from io import StringIO
from pydantic import BaseModel
from enum import Enum


class UpdateBody(BaseModel):
    passenger_id: int
    properties_to_change: dict[str, Union[str, int]]


app = FastAPI()

db_column_names = set(["survived", "name", "sex", "age", "ticket", "cabin"])


@app.post("/uploadcsv")
async def load_data(file: UploadFile):
    if file.content_type != "text/csv":
        raise HTTPException(status_code=422, detail="The uploaded file must be a csv!")
    file_contents = await file.read()
    buffer = StringIO(file_contents.decode("utf-8"))
    csv_reader = csv.DictReader(buffer)

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
    cursor.close()
    conn.commit()
    conn.close()
    return {"type": file.content_type, "data": {}}


@app.get("/data")
def get_data():
    conn = sqlite3.connect("titanic.db")
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM titanic_data")
    except Exception as e:
        raise HTTPException(
            status_code=410, detail="The titanic_data table does not exist"
        )

    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


@app.delete("/delete")
def delete_table():
    conn = sqlite3.connect("titanic.db")
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS titanic_data")
    cursor.close()
    conn.commit()
    conn.close()

    return {"status": "success"}


@app.get("/survived")
def get_survived_persons():
    conn = sqlite3.connect("titanic.db")

    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT id, name, sex, age
        FROM titanic_data
        WHERE survived=1
        """
    )

    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows


@app.patch("/update")
def update_row(body: UpdateBody):
    for key in body.properties_to_change:
        if key not in db_column_names:
            raise HTTPException(
                status_code=422,
                detail=f"One of the properties that you passed into the properties_to_change is not valid. The valid properties are {db_column_names}",
            )

    conn = sqlite3.connect("titanic.db")
    cursor = conn.cursor()

    set_clause = ", ".join([f"{key} = ?" for key in body.properties_to_change])

    cursor.execute(
        f"""
        UPDATE titanic_data
        SET {set_clause}
        WHERE id = ?;
        """,
        list(body.properties_to_change.values()) + [body.passenger_id],
    )
    cursor.close()
    conn.commit()
    conn.close()

    return {"success": True}
