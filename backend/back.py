from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import boto3
import pandas as pd
from io import StringIO
import csv
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

app = FastAPI()

# Allow CORS from frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ak = os.getenv("keyid")
sk = os.getenv("key")
region = os.getenv("region")


s3 = boto3.client("s3",
        aws_access_key_id=ak,
        aws_secret_access_key=sk)
BUCKET_NAME = "lol-stats-data"

@app.get("/tournaments")
def list_tournaments():
    objects = s3.list_objects_v2(Bucket=BUCKET_NAME)
    
    if "Contents" not in objects:
        return {"tournaments": []}
    
    tournaments = []
    for obj in objects["Contents"]:
        key = obj["Key"]
        if key.endswith(".csv"):
            name = key.replace(".csv", "")
            tournaments.append(name)
    return {"tournaments": tournaments}

@app.get("/tournament/{tournament_name}")
def get_tournament_players(tournament_name: str):
    key = f"{tournament_name}.csv"

    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        data = obj["Body"].read().decode("utf-8")
        df = pd.read_csv(StringIO(data))
        df = df.fillna("")
        return {"players": df.to_dict(orient="records")}
    except s3.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail="Tournament data not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)