import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from datetime import datetime, timedelta

# 🔧 Конфигурация Grafana
GRAFANA_URL = "https://grafana.payda.online"
GRAFANA_API_KEY = os.getenv("GRAFANA_API_KEY")
GRAFANA_DATASOURCE_UID = "ce37vo70kfcaob"

# 🔧 Google Sheets
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
CREDENTIALS_FILE = "credentials.json"
SHEET_NAME = "unique drivers main"

def fetch_data():
    headers = {
        "Authorization": f"Bearer {GRAFANA_API_KEY}"
    }

    raw_sql = '''
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
          WHERE a.document_date >= '2025-06-01'
            AND a.buyer_name = 'ТОО "Y. Taxi Qazaqstan"'
          UNION ALL
          SELECT a.buyer_name,
                 a.buyer_tin,
                 a.buyer_phone,
                 a.pdf_id
          FROM awps a
          WHERE a.document_date >= '2025-06-01'
            AND a.seller_name = 'ТОО "Y. Taxi Qazaqstan"'
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
        ORDER BY sub.tin, (COALESCE(esf_file.current_status, '') = '') ASC
    '''

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
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)

    sheet = client.open_by_key(GOOGLE_SHEET_ID).worksheet(SHEET_NAME)
    meta = client.open_by_key(GOOGLE_SHEET_ID).worksheet("last update")

    print("🚀 Получаем данные из Grafana...")
    data = fetch_data()
    values = data["results"]["A"]["frames"][0]["data"]["values"]
    rows = list(zip(*values))

    headers = ["name", "tin", "phone", "park_full_name", "Статус АВР", "Статус ЭСФ"]

    print("🧼 Очищаем Google Sheet...")
    sheet.clear()

    print("📥 Вставляем заголовки...")
    sheet.update("A1:F1", [headers])

    print(f"📊 Загружаем {len(rows)} строк...")
    if rows:
        sheet.append_rows(rows, value_input_option="RAW")
        print(f"✅ Успешно залито {len(rows)} строк в '{SHEET_NAME}'")

    # Алматы +5 UTC
    almaty_time = datetime.utcnow() + timedelta(hours=5)
    almaty_str = almaty_time.strftime("%Y-%m-%d %H:%M:%S")
    meta.update(range_name="A1", values=[[almaty_str]])
    print(f"🕓 Время последней загрузки обновлено: {almaty_str}")

if __name__ == "__main__":
    update_sheet()
