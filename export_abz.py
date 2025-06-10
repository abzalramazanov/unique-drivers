import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime, timezone

GRAFANA_URL = "https://grafana.payda.online"
GRAFANA_API_KEY = os.getenv("GRAFANA_API_KEY")
GRAFANA_DATASOURCE_UID = "ce37vo70kfcaob"

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SHEET_NAME = "unique drivers main"
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json")

def fetch_data(from_timestamp):
    headers = {
        "Authorization": f"Bearer {GRAFANA_API_KEY}"
    }
    raw_sql = """
        SELECT DISTINCT ON (sub.tin)
               sub.name,
               sub.tin,
               sub.phone,
               park_info.full_name AS park_full_name,
               COALESCE(pdf_file.current_status, 'n/a') AS "Статус АВР",
               COALESCE(esf_file.current_status, 'n/a') AS "Статус ЭСФ"
        FROM (
          SELECT a.seller_name AS name,
                 a.seller_tin  AS tin,
                 a.seller_phone AS phone,
                 a.pdf_id
          FROM awps a
          WHERE a.document_date >= '2025-05-01'
            AND a.buyer_name = 'ТОО "Яндекс.Такси Корп"'

          UNION ALL

          SELECT a.buyer_name,
                 a.buyer_tin,
                 a.buyer_phone,
                 a.pdf_id
          FROM awps a
          WHERE a.document_date >= '2025-05-01'
            AND a.seller_name = 'ТОО "Яндекс.Такси Корп"'
        ) sub
        LEFT JOIN documents pdf_file ON pdf_file.id::text = sub.pdf_id
        LEFT JOIN esfs esf ON esf.related_document_id::text = pdf_file.id::text
        LEFT JOIN documents esf_file ON esf_file.id::text = esf.pdf_id
        LEFT JOIN dblink(
          'host=172.16.88.11 dbname=production_payda user=tech_read_user password=AqIXe52814BvKIWIBbpM',
          'SELECT tax_id_number, park_id FROM fleet_driver_credential_detail'
        ) AS driver_info(tax_id_number VARCHAR, park_id INT)
          ON driver_info.tax_id_number = sub.tin
        LEFT JOIN dblink(
          'host=172.16.88.11 dbname=production_payda user=tech_read_user password=AqIXe52814BvKIWIBbpM',
          'SELECT id, full_name FROM users_client'
        ) AS park_info(id INT, full_name TEXT)
          ON park_info.id = driver_info.park_id
        ORDER BY sub.tin
    """

    payload = {
        "queries": [
            {
                "refId": "A",
                "datasource": {
                    "uid": GRAFANA_DATASOURCE_UID,
                    "type": "grafana-postgresql-datasource"
                },
                "rawSql": raw_sql,
                "format": "table"
            }
        ],
        "from": "now-30d",
        "to": "now"
    }

    response = requests.post(f"{GRAFANA_URL}/api/ds/query", headers=headers, json=payload)
    response.raise_for_status()
    return response.json()

def update_sheet():
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_NAME)
    last_update_ws = client.open_by_key(GOOGLE_SHEET_ID).worksheet("last update")

    data = fetch_data("2025-05-01 00:00:00")
    table = data["results"]["A"]["frames"][0]["data"]["values"]
    rows = list(zip(*table))

    sheet.append_rows(rows, value_input_option="RAW")

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    last_update_ws.update("A1", [[now_str]])

if __name__ == "__main__":
    update_sheet()