# googlesheetsync

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from sqlalchemy import create_engine, Column, Integer, String, Numeric, inspect, text
from sqlalchemy.orm import declarative_base, sessionmaker
from apscheduler.schedulers.background import BackgroundScheduler
import time
from decimal import Decimal
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Google Sheets Configuration
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
CREDS_FILE = "credentials.json"
SPREADSHEET_ID = "15FQxo2ZpIRdHcxuRvvNq6kUrWbjyFewE3vFxhSgCnk4"

# MySQL Configuration
DB_USER = 'root'
DB_PASSWORD = 'Yashu4593*?'
DB_HOST = '127.0.0.1'
DB_PORT = '3306'
DB_NAME = 'google_sheet_sync'

# SQLAlchemy Setup
Base = declarative_base()

class DataRecord(Base):
    __tablename__ = 'employees'
    employee_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    department = Column(String(100))
    salary = Column(Numeric(10, 2))

# Create MySQL engine
engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}', echo=False)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

# Google Sheets Client
def get_google_sheets_client():
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
        return gspread.authorize(credentials)
    except Exception as e:
        logging.error(f"Error authorizing Google Sheets client: {e}")
        return None

# Function to update database schema based on Google Sheets
def update_database_schema(headers):
    inspector = inspect(engine)
    existing_columns = {column['name'] for column in inspector.get_columns(DataRecord.__tablename__)}

    for header in headers:
        if header not in existing_columns:
            logging.info(f"Adding new column to database: {header}")
            with engine.connect() as connection:
                # Use SQL text for MySQL
                connection.execute(text(f'ALTER TABLE employees ADD COLUMN `{header}` VARCHAR(255)'))

# Sync Google Sheets to MySQL
def sync_gsheet_to_db():
    session = Session()
    try:
        gc = get_google_sheets_client()
        if gc is None:
            logging.error("Google Sheets client is not initialized.")
            return

        sheet = gc.open_by_key(SPREADSHEET_ID).sheet1
        records = sheet.get_all_records()

        if not records:
            logging.warning("No records found in Google Sheets.")
            return

        # Update DB schema for new columns
        update_database_schema(sheet.row_values(1))

        gsheet_employee_ids = set()
        for record in records:
            emp_id = record.get('employee_id')
            if emp_id is not None and str(emp_id).isdigit():
                gsheet_employee_ids.add(int(emp_id))

        db_records = session.query(DataRecord).all()
        db_employee_ids = {r.employee_id for r in db_records}

        # Do NOT delete any record as per your latest request

        # Insert/Update records
        for record in records:
            emp_id = record.get('employee_id')
            if emp_id is None or not str(emp_id).isdigit():
                continue
            emp_id = int(emp_id)
            existing = session.query(DataRecord).filter_by(employee_id=emp_id).first()

            if existing:
                existing.name = record.get('name', existing.name)
                existing.department = record.get('department', existing.department)
                existing.salary = record.get('salary', existing.salary)

                for key, value in record.items():
                    if key not in ['employee_id', 'name', 'department', 'salary']:
                        setattr(existing, key, value)
                logging.info(f"Updated: {emp_id}")
            else:
                new = DataRecord(
                    employee_id=emp_id,
                    name=record.get('name'),
                    department=record.get('department'),
                    salary=record.get('salary')
                )
                for key, value in record.items():
                    if key not in ['employee_id', 'name', 'department', 'salary']:
                        setattr(new, key, value)
                session.add(new)
                logging.info(f"Added: {emp_id}")

        session.commit()
    except Exception as e:
        logging.error(f"Error syncing Google Sheets to MySQL: {e}")
        session.rollback()
    finally:
        session.close()

# Sync MySQL to Google Sheets
def sync_db_to_gsheet():
    session = Session()
    try:
        gc = get_google_sheets_client()
        if gc is None:
            logging.error("Google Sheets client is not initialized.")
            return

        sheet = gc.open_by_key(SPREADSHEET_ID).sheet1
        records = session.query(DataRecord).all()

        headers = ['employee_id', 'name', 'department', 'salary']
        data = [headers]
        for rec in records:
            salary = float(rec.salary) if isinstance(rec.salary, Decimal) else rec.salary
            data.append([rec.employee_id, rec.name, rec.department, salary])

        # Overwrite sheet content
        sheet.clear()
        sheet.insert_rows(data)
        logging.info("Database synced to Google Sheets.")
    except Exception as e:
        logging.error(f"Error syncing DB to Google Sheets: {e}")
    finally:
        session.close()

# Schedule jobs
scheduler = BackgroundScheduler()
scheduler.add_job(sync_gsheet_to_db, 'interval', seconds=60)
scheduler.add_job(sync_db_to_gsheet, 'interval', seconds=60)
scheduler.start()

try:
    logging.info("Sync service running... Press Ctrl+C to stop.")
    while True:
        time.sleep(1)
except (KeyboardInterrupt, SystemExit):
    scheduler.shutdown()
    logging.info("Sync service stopped.")
