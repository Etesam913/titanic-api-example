## Usage

- Run the server

```bash
uvicorn main:app --reload
```

- To load the data from the csv make a post request to the `/uploadcsv` endpoint with the `titanic.csv` data in the request
