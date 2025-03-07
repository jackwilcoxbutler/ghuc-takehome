from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.utils.task_group import TaskGroup

import hashlib
import logging
import os
import dotenv
import yaml
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import tempfile
import re

# Load environment variables from .env file
dotenv.load_dotenv()

# Load configuration from config.yaml
with open(os.path.expanduser('~/airflow/dags/config.yaml')) as f:
    CONFIG = yaml.safe_load(f)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 4),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1
}

DATASET_MAP = {
    'patients.csv': 'patients',
    'visits.csv': 'visits',
    'lab_results.csv': 'lab_results',
    'icd_reference.csv': 'icd_reference'
}
bq_schemas = {
    "patients": [
        bigquery.SchemaField("patient_id", "STRING"),
        bigquery.SchemaField("first_name", "STRING"),
        bigquery.SchemaField("last_name", "STRING"),
        bigquery.SchemaField("date_of_birth", "DATETIME"),
        bigquery.SchemaField("gender", "STRING"),
        bigquery.SchemaField("address", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("zip", "STRING"),
        bigquery.SchemaField("phone", "STRING"),
        bigquery.SchemaField("insurance_id", "STRING"),
        bigquery.SchemaField("insurance_effective_date", "DATETIME"),
        bigquery.SchemaField("effective_start_date", "DATETIME"),
        bigquery.SchemaField("effective_end_date", "DATETIME"),
        bigquery.SchemaField("is_current", "BOOLEAN"),
    ],
    "lab_results": [
        bigquery.SchemaField("lab_id", "STRING"),
        bigquery.SchemaField("visit_id", "STRING"),
        bigquery.SchemaField("test_name", "STRING"),
        bigquery.SchemaField("test_value", "STRING"),
        bigquery.SchemaField("test_units", "STRING"),
        bigquery.SchemaField("reference_range", "STRING"),
        bigquery.SchemaField("date_performed", "DATETIME"),
        bigquery.SchemaField("date_resulted", "DATETIME"),
        bigquery.SchemaField("effective_start_date", "DATETIME"),
        bigquery.SchemaField("effective_end_date", "DATETIME"),
        bigquery.SchemaField("is_current", "BOOLEAN"),
    ],
    "icd_reference": [
        bigquery.SchemaField("icd_code", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("effective_date", "DATETIME"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("effective_start_date", "DATETIME"),
        bigquery.SchemaField("effective_end_date", "DATETIME"),
        bigquery.SchemaField("is_current", "BOOLEAN"),
    ],
    "visits": None  # Define this later if needed
}


raw_bucket = CONFIG['zones']['raw']['bucket']

def _clean_csv(file_path):
    tempfile = CONFIG['tempfile_path']
    cleaned_file_path = tempfile
    with open(file_path, 'r', encoding = 'utf-8-sig') as infile, open(cleaned_file_path, 'w', encoding='utf-8') as outfile:
        for line in infile:
            #Remove whitespace
            line = line.strip()

            #Remove wrapping quote
            if line.startswith('"') and line.endswith('"'):
                line = line[1:-1]

            
            line = line.replace('""','"')
        
            outfile.write(line + "\n")
    return cleaned_file_path


def _process_raw_files(**context):
    sftp_hook = SFTPHook('SFTP_CONN')
    execution_date = context['ds']

    processed_files = []
    logging.info(f"Starting raw file processing for execution date: {execution_date}")

    try:
        # Open the SFTP connection once using the context manager for the entire task
        with sftp_hook.get_conn() as sftp_client:
            logging.info(f"SFTP connection established.")

            # Initialize the GCS client
            client = storage.Client()
            bucket = client.get_bucket(raw_bucket)

            for remote_file, dataset in DATASET_MAP.items():
                remote_path = f"/{execution_date}/{remote_file}"

                logging.info(f"Attempting to download file from SFTP: {remote_path}")

                # Use tempfile to create a temporary file to store the downloaded content
                with tempfile.NamedTemporaryFile(delete=False, mode='wb') as local_temp:
                    sftp_client.get(remote_path, local_temp.name)
                    logging.info(f"Successfully downloaded file: {remote_file}")

                    # Hash calculation for downloaded file
                    logging.info(f"Calculating hash for downloaded file: {remote_file}")
                    with open(local_temp.name, 'rb') as f:
                        downloaded_hash = hashlib.sha256(f.read()).hexdigest()
                    logging.info(f"Calculated hash for downloaded file: {downloaded_hash}")

                    # GCS Upload
                    gcs_path = f"{dataset}/{execution_date}/{remote_file}"
                    logging.info(f"Uploading file to GCS raw bucket: {gcs_path}")

                    # Upload file to GCS using the storage client
                    blob = bucket.blob(gcs_path)
                    blob.upload_from_filename(local_temp.name)
                    logging.info(f"Successfully uploaded {remote_file} to GCS path: {gcs_path}")

                    # Download file from GCS for hash verification
                    with tempfile.NamedTemporaryFile(delete=False, mode='wb') as local_temp_gcs:
                        logging.info(f"Downloading file from GCS for hash verification: gs://{raw_bucket}/{gcs_path}")
                        blob.download_to_filename(local_temp_gcs.name)

                        # Hash calculation for uploaded file
                        logging.info(f"Calculating hash for uploaded file from GCS: {remote_file}")
                        with open(local_temp_gcs.name, 'rb') as f:
                            uploaded_hash = hashlib.sha256(f.read()).hexdigest()
                        logging.info(f"Calculated hash for uploaded file: {uploaded_hash}")

                        # Compare the downloaded file hash and uploaded file hash
                        if downloaded_hash == uploaded_hash:
                            logging.info(f"Hash verification successful for {remote_file}. Hash: {downloaded_hash}")
                        else:
                            logging.error(f"Hash mismatch for {remote_file}: Downloaded Hash: {downloaded_hash}, Uploaded Hash: {uploaded_hash}")
                            raise Exception(f"Hash mismatch for {remote_file}")

                    processed_files.append(gcs_path)

                    # Cleanup temp files
                    os.remove(local_temp.name)
                    os.remove(local_temp_gcs.name)
                    logging.info(f"Removed temporary local files: {local_temp.name}, {local_temp_gcs.name}")

    except Exception as e:
        logging.error(f"Error processing files: {e}")
        raise

    logging.info(f"Processed files: {processed_files}")

def _read_csv_from_gcs(bucket_name, dataset_name, file_name, execution_date):
    # Initialize GCS client
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(f"{dataset_name}/{execution_date}/{file_name}")

    # Download the CSV content as bytes
    content_bytes = blob.download_as_bytes()

    # Create a temporary file to save the CSV content
    with tempfile.NamedTemporaryFile(delete=False, mode='wb') as dirty_file:
        dirty_file.write(content_bytes)
        dirty_file_path = dirty_file.name  # Save the path of the temporary file

    # Call _read_complex_csv with the temporary file path
    cleaned_csv = _clean_csv(dirty_file_path)
    df = pd.read_csv(cleaned_csv, quotechar= '"')
    logging.info(df)


    # Optionally delete the temporary file after reading it
    os.remove(dirty_file_path)
    os.remove(cleaned_csv)

    return df

def _clean_csv(dirty_file_path):
    tempfile = CONFIG['tempfile_path']
    cleaned_file_path = tempfile
    with open(dirty_file_path, 'r', encoding = 'utf-8-sig') as infile, open(cleaned_file_path, 'w', encoding='utf-8') as outfile:
        for line in infile:
            #Remove whitespace
            line = line.strip()

            #Remove wrapping quote
            if line.startswith('"') and line.endswith('"'):
                line = line[1:-1]

            
            line = line.replace('""','"')
        
            outfile.write(line + "\n")
    return cleaned_file_path



def _validate_date(date_str, date_format='%Y-%m-%d'):
    """Validate date string format (YYYY-MM-DD) or None."""
    if pd.isna(date_str):
        return True
    return date_str.strftime(date_format) == date_str.strftime(date_format)



def _validate_patient_data(df):
    """Validate Patient Data CSV."""
    errors = []
    for index, row in df.iterrows():
        if pd.isna(row['patient_id']):
            errors.append(f"Patient {row['patient_id']}: Missing required fields")
        if not _validate_date(row['date_of_birth']):
            errors.append(f"Patient {row['patient_id']}: Invalid date_of_birth format")
        if pd.notna(row['phone']) and isinstance(row['phone'], str):
            phone = row['phone'].strip()  
            if phone and not re.match(r'\(\d{3}\)\d{3}-\d{4}', phone):
                errors.append(f"Patient {row['patient_id']}: Invalid phone number format")
    return errors

def _validate_visit_data(df):
    """Validate Visit Data CSV."""
    errors = []
    logging.info(df[['visit_id', 'patient_id','visit_date']])
    for index, row in df.iterrows():
        if pd.isna(row['visit_id']) or pd.isna(row['patient_id']):
            errors.append(f"Visit {row['visit_id']}: Missing required fields")
        if not _validate_date(row['visit_date']):
            errors.append(f"Visit {row['visit_id']}: Invalid visit_date format")
        if not pd.isna(row['billable_amount']) and not isinstance(row['billable_amount'], (int, float)):
            errors.append(f"Visit {row['visit_id']}: Invalid billable_amount")
    return errors

def _validate_lab_results(df):
    """Validate Lab Results CSV."""
    errors = []
    for index, row in df.iterrows():
        if pd.isna(row['lab_id']) or pd.isna(row['visit_id']) or pd.isna(row['test_name']):
            errors.append(f"Lab Result {row['lab_id']}: Missing required fields")
        if not pd.isna(row['date_performed']) and not _validate_date(row['date_performed']):
            errors.append(f"Lab Result {row['lab_id']}: Invalid date_performed format")
    return errors

def _validate_icd_reference(df):
    """Validate ICD Reference CSV."""
    errors = []
    for index, row in df.iterrows():
        if pd.isna(row['icd_code']) or pd.isna(row['description']) or pd.isna(row['effective_date']):
            errors.append(f"ICD Code {row['icd_code']}: Missing required fields")
        if not _validate_date(row['effective_date']):
            errors.append(f"ICD Code {row['icd_code']}: Invalid effective_date format")
    return errors

def _write_df_to_gcs_temp(bucket_name, file_path, df):
    """Writes a pandas DataFrame to a temp CSV file and uploads it to GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=True) as temp_file:
        df.to_csv(temp_file.name, index=False)
        temp_file.seek(0)
        blob.upload_from_filename(temp_file.name, content_type="text/csv")

    print(f"File uploaded to gs://{bucket_name}/{file_path}")

def _validate_data(**kwargs):
    """Validate transformed CSV files using XCom-pulled data."""

    ti = kwargs['ti']
    execution_date = kwargs['ds']
    dataset_name = kwargs['dataset_name']
    file_name = kwargs['file_name']
    validation_func = kwargs['validation_func']

    all_errors = {}

    try:
        # Define the path to the CSV file in GCS
        staging_bucket = CONFIG['zones']['staging']['bucket']
        gcs_path = f"staging/{dataset_name}/{execution_date}/{dataset_name}.csv"
        
        df = _read_csv_from_gcs(staging_bucket, dataset_name, file_name, execution_date)

        # Perform validation
        errors = validation_func(df)

        del df

        if errors:
            all_errors[file_name] = errors
            logging.warning(f"Validation errors found in {file_name}")
        else:
            logging.info(f"No validation errors found in {file_name}, proceeding")
         
    
    except Exception as e:
        logging.error(f"Error validating {file_name}: {str(e)}")
        all_errors[file_name] = [f"Validation error: {str(e)}"]

    return all_errors


def _parse_date(date_str):
    """Parse dates using pandas' built-in parsing, flag invalid dates, and return standardized output."""

    if pd.isna(date_str) or date_str.strip() == "":
        return pd.NaT
    
    date_str.strip()

    # Try parsing the date using pandas' to_datetime with errors='coerce' to handle multiple formats
    parsed_date = pd.to_datetime(date_str, errors='coerce', dayfirst=False)  # dayfirst=False by default
    
    # If parsing fails, return NaT
    return parsed_date if not pd.isna(parsed_date) else pd.NaT

def _parse_phone_number(phone):
    """Format phone number to (xxx)xxx-xxxx."""
    phone = re.sub(r'\D', '', str(phone))  # Remove non-digit characters
    if len(phone) == 10:
        return f"({phone[:3]}){phone[3:6]}-{phone[6:]}"
    return None


def _transform_patient_data(df):
    """Transform Patient Data CSV."""

    # Apply the parsing function to the date columns
    df['date_of_birth'] = df['date_of_birth'].apply(_parse_date)
    df['insurance_effective_date'] = df['insurance_effective_date'].apply(_parse_date)
    
    # Set invalid or missing `date_of_birth` to NaN
    df['date_of_birth'] = df['date_of_birth'].where(df['date_of_birth'].notna(), None)
    
    # Normalize `gender` (capitalize first letter) and set invalid gender to NaN
    df['gender'] = df['gender'].str.strip().str.upper()
    df['gender'] = df['gender'].where(df['gender'].isin(['M', 'F', 'O']), None) 
    
    # Normalize phone numbers: Remove all non-numeric characters and set invalid phone numbers to NaN
    df['phone'] = df['phone'].apply(_parse_phone_number)
    
    # Normalize names (capitalize first letter of each word) and set invalid names to NaN
    df['first_name'] = df['first_name'].apply(lambda x: None if pd.isna(x) or x == '' else x)
    df['last_name'] = df['last_name'].apply(lambda x: None if pd.isna(x) or x == '' else x)

    df['first_name'] = df['first_name'].str.strip().str.title()
    df['last_name'] = df['last_name'].str.strip().str.title()


    df['city'] = df['city'].apply(lambda x: None if pd.isna(x) or x == '' else x)
    df['address'] = df['address'].apply(lambda x: None if pd.isna(x) or x == '' else x)

    # Convert the 'zip' column to a string and remove decimal places
    df['zip'] = df['zip'].astype('Int64').astype(str) 
    df['zip'] = df['zip'].replace({r'\D': ''}, regex=True)
    df['zip'] = df['zip'].apply(lambda x: x if len(x) == 5 else None)
    df['zip'] = df['zip'].apply(lambda x: x.zfill(5) if pd.notna(x) else None)


    df['state'] = df['state'].astype(str).str.strip().str.upper()
    df['state'] = df['state'].apply(lambda x: x if len(x) == 2 and x.isalpha() else None)


    # Handle missing insurance ID and set invalid insurance ID to NaN
    df['insurance_id'] = df['insurance_id'].where(df['insurance_id'].notna() & (df['insurance_id'] != ''), None)

    return df

def split_reason_and_icd(row):
    """Handle and split reason for visit and ICD code if they are combined."""
    if isinstance(row['reason_for_visit'], str) and ',' in row['reason_for_visit']:
        # Split the string by comma
        reason, icd_code = row['reason_for_visit'].split(',', 1)
        row['reason_for_visit'] = reason.strip()
        row['icd_code'] = icd_code.strip() 
    return row

def _transform_visit_data(df):
    """Transform Visit Data CSV."""
    df = df.apply(split_reason_and_icd, axis=1)
    # Fix date formats (example: standardizing visit_date format)
    df['visit_date'] = df['visit_date'].apply(_parse_date)
    df['follow_up_date'] = df['follow_up_date'].apply(_parse_date)

    # Normalize 'billable_amount' (ensure it's a float and fill missing values)
    df['billable_amount'] = pd.to_numeric(df['billable_amount'], errors='coerce')
    df['billable_amount'] = df['billable_amount'].fillna(0.0)

    df['currency'] = df['currency'].apply(lambda x: None if pd.isna(x) or pd.isnull(x) else x)

    
    return df


def _transform_lab_result_data(df):
    """Transform Lab Result Data CSV."""
    # Fix date formats (example: standardizing date_performed format)
    df['date_performed'] = df['date_performed'].apply(_parse_date)
    df['date_resulted'] = df['date_resulted'].apply(_parse_date)
    
    return df


def _transform_icd_reference_data(df):
    """Transform ICD Reference Data CSV."""
    # Fix date formats (example: standardizing effective_date format)
    df['effective_date'] = df['effective_date'].apply(_parse_date)
    
    return df

 # Define transformation functions for each dataset
transformation_functions = {
    'patients.csv': _transform_patient_data,
    'visits.csv': _transform_visit_data,
    'lab_results.csv': _transform_lab_result_data,
    'icd_reference.csv': _transform_icd_reference_data
}

def _transform_data(**kwargs):
    """Transform all CSV data before validation."""
    execution_date = kwargs['ds']  
    file_name = kwargs['file_name']
    dataset_name = kwargs['dataset_name']
    transform_func = kwargs['transform_func']

    # Iterate over each dataset and apply the transformation
    logging.info(f"Transforming {file_name}")
    try:
        # Read CSV from GCS
        df = _read_csv_from_gcs(raw_bucket, dataset_name, file_name, execution_date)
        logging.info(f"read {file_name} from GCS, starting transform")

        
        # Apply transformation to data
        transformed_df = transform_func(df)
        logging.info(f"Transformed {file_name} successfully")

        if dataset_name in ['patients', 'lab_results', 'icd_reference']:
            # Adding default values for SCD fields
            transformed_df['effective_start_date'] = pd.to_datetime(execution_date)  # Set start date to execution date
            transformed_df['effective_end_date'] = pd.NaT  # Null end date initially
            transformed_df['effective_end_date'] = transformed_df['effective_end_date'].astype('datetime64[ns]')
            transformed_df['is_current'] = True  # New records are always current

        # Upload the temporary file to GCS
        staging_bucket = CONFIG['zones']['staging']['bucket']
        staging_file = f"{dataset_name}/{execution_date}/{dataset_name}.csv"
        
        _write_df_to_gcs_temp(staging_bucket, staging_file, transformed_df)

        del transformed_df
    except Exception as e:
        logging.error(f"Error transforming {file_name}: {str(e)}")
        raise Exception(f"Error transforming {file_name}: {str(e)}")
    
def _load_to_bigquery(**kwargs):
    """Load files from gcs to BigQuery"""
    execution_date = kwargs['ds']
    staging_bucket = CONFIG['zones']['staging']['bucket']
    bq_stg_project = CONFIG['zones']['staging']['project']  
    bq_stg_dataset = CONFIG['zones']['staging']['dataset']  
    bq_cur_project = CONFIG['zones']['curated']['project']  
    bq_cur_dataset = CONFIG['zones']['curated']['dataset']  

    dataset_name = kwargs['dataset_name']
    file_name = kwargs['file_name']
    
    # Get configuration values
    if dataset_name == 'visits':
        table_prefix = 'fact'
    else:
        table_prefix = 'dim'
    
    # Construct identifiers
    gcs_uri = f"gs://{staging_bucket}/{dataset_name}/{execution_date}/{file_name}"
    table_id = f"{bq_stg_project}.{bq_stg_dataset}.{table_prefix}_{dataset_name}"
    schema = bq_schemas.get(dataset_name, None)

    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True if schema is None else False,  # Disable auto-detect if schema is provided
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=schema  # Use the custom schema for patients.csv
    )
    
    try:
        client = bigquery.Client()
        load_job = client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )
        load_job.result()
        
        if load_job.errors:
            raise RuntimeError(f"Load job failed: {load_job.errors}")
            
        logging.info(f"Loaded {load_job.output_rows} rows to {table_id}")
        
        # Step 2: Read the external SQL file and apply SCD Type 2 logic
        if dataset_name == "visits":
            return
        
        sql_file = f'/Users/jackwilcox/airflow/dags/scd_{dataset_name}.sql'

        # Read the SCD logic from the external SQL file
        with open(sql_file, 'r') as sql_file_content:
            scd_type_2_sql = sql_file_content.read()

            # Substitute the dataset and table names in the SQL
            scd_type_2_sql = scd_type_2_sql.format(
                bq_stg_project=bq_stg_project,
                bq_stg_dataset=bq_stg_dataset,
                bq_cur_project=bq_cur_project,
                bq_cur_dataset=bq_cur_dataset
            )

            # Execute the SCD Type 2 SQL query
            query_job = client.query(scd_type_2_sql)
            query_job.result()

            logging.info(f"SCD Type 2 logic applied successfully for {dataset_name}")

            
    except Exception as e:
        logging.error(f"Failed to load {file_name}: {str(e)}")
        raise

validation_functions = {
    'patients.csv': _validate_patient_data,
    'visits.csv': _validate_visit_data,
    'lab_results.csv': _validate_lab_results,
    'icd_reference.csv': _validate_icd_reference
}

with DAG(
    'raw_zone_ingestion',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    template_searchpath=['/etc/airflow/include'],
    doc_md="""### Raw Zone Ingestion Pipeline
    **Flow**:
    1. Read files from dated local folder
    2. Validate and upload to Raw Zone GCS
    3. Maintain dataset separation
    """
) as dag:

    # Start Dummy Operator task
    start_task = DummyOperator(
        task_id='start',
        dag=dag
    )
    logging.info("DAG started.")

    # Ingestion task
    ingestion_task = PythonOperator(
        task_id='process_raw_files',
        python_callable=_process_raw_files,
        execution_timeout=timedelta(minutes=10),
        dag=dag
    )

    with TaskGroup("transformation_tasks") as transformation_tasks:
       
        load_tasks = [
            PythonOperator(
                task_id=f"transform_{dataset_name}",
                python_callable = _transform_data,
                op_kwargs={'dataset_name': dataset_name, 'file_name': file_name,'transform_func': transformation_functions.get(file_name)},
            )
            for file_name,dataset_name in DATASET_MAP.items()
        ]
        
    with TaskGroup("validation_tasks") as validation_task_group:
        validation_tasks = [
            PythonOperator(
                task_id=f"validate_{dataset_name}",
                python_callable=_validate_data,
                op_kwargs={'dataset_name': dataset_name, 'file_name': file_name, 'validation_func' : validation_functions.get(file_name)},
                execution_timeout=timedelta(minutes=10),
                dag=dag
            )
            for file_name, dataset_name in DATASET_MAP.items()
        ]

    # Define a TaskGroup for BigQuery loading tasks
    with TaskGroup("bq_loading_tasks") as bq_loading_tasks:
        
        load_tasks = [
            PythonOperator(
                task_id=f"load_{dataset_name}_to_gbq",
                python_callable=_load_to_bigquery,
                op_kwargs={'dataset_name': dataset_name, 'file_name': file_name}
            )
            for file_name,dataset_name in DATASET_MAP.items()
        ]


    # End Dummy Operator task
    end_task = DummyOperator(
        task_id='end',
        dag=dag
    )
    logging.info("DAG finished.")

start_task  >> ingestion_task >> transformation_tasks >> validation_tasks >> bq_loading_tasks >> end_task
