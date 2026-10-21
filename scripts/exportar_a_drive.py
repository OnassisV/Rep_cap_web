import os, io, json
import pandas as pd
from sqlalchemy import create_engine
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

TABLAS = [t.strip() for t in os.environ["TABLAS"].split(",")]
FOLDER_ID = os.environ["DRIVE_FOLDER_ID"]

db_url = os.environ["DATABASE_URL"].strip()
if db_url.startswith("mysql://"):
    db_url = db_url.replace("mysql://", "mysql+pymysql://", 1)
engine = create_engine(db_url)  # mysql+pymysql://user:pass@host:port/db
info = json.loads(os.environ["GOOGLE_SA_JSON"])
creds = service_account.Credentials.from_service_account_info(
    info, scopes=["https://www.googleapis.com/auth/drive"])
drive = build("drive", "v3", credentials=creds)

for t in TABLAS:
    df = pd.read_sql(f"SELECT * FROM `{t}`", engine)
    data = io.BytesIO(df.to_csv(index=False).encode("utf-8-sig"))
    media = MediaIoBaseUpload(data, mimetype="text/csv")
    q = f"name='{t}.csv' and '{FOLDER_ID}' in parents and trashed=false"
    ex = drive.files().list(q=q, fields="files(id)").execute()["files"]
    if not ex:
        print(f"FALTA {t}.csv: crea un archivo vacio con ese nombre en la carpeta de Drive")
        continue
    drive.files().update(fileId=ex[0]["id"], media_body=media).execute()
    print(f"OK {t}: {len(df)} filas")