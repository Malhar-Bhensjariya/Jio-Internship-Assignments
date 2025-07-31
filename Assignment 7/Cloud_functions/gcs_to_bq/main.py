import os
import csv
import re
import time
import logging
from datetime import datetime
from google.cloud import bigquery, storage
from google.api_core.exceptions import NotFound, Conflict

# Import modular components
from BQ_SQL.data_quality_analyzer import DataQualityAnalyzer
from BQ_SQL.data_cleaner import DataCleaner
from BQ_SQL.backup_manager import BackupManager
from BQ_SQL.validator import DataValidator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_column(name):
    """Clean column names for BigQuery compatibility"""
    return re.sub(r'[^a-zA-Z0-9_]', '_', name).strip('_').lower()

def detect_column_type(values):
    """
    Robust type detection for CSV values with the following logic:
    1. First check for boolean (0/1, true/false, yes/no)
    2. Then check for integer (only if no decimal points in string representation)
    3. Then check for float
    4. Default to string
    """
    if not values:
        return 'STRING'
    
    # Remove None/empty values for analysis
    non_null = [str(v).strip() for v in values if v not in [None, '']]
    if not non_null:
        return 'STRING'
    
    # Boolean detection (case-insensitive)
    bool_pattern = re.compile(r'^(true|false|yes|no|t|f|y|n|0|1)$', re.IGNORECASE)
    if all(bool_pattern.match(v) for v in non_null):
        return 'BOOLEAN'
    
    # Integer detection - only if string doesn't contain decimal point
    int_count = 0
    for v in non_null:
        # Skip if value contains decimal point in string representation
        if '.' in v:
            break
        try:
            int(v)
            int_count += 1
        except ValueError:
            break
    
    if int_count == len(non_null):
        return 'INT64'
    
    # Float detection
    float_count = 0
    for v in non_null:
        try:
            float(v)
            float_count += 1
        except ValueError:
            break
    
    if float_count == len(non_null):
        return 'FLOAT64'
    
    return 'STRING'

def process_csv_schema(storage_client, bucket_name, file_name, sample_size=100):
    """
    Process CSV to extract schema with proper typing
    Analyzes a sample of rows to determine column types
    """
    try:
        blob = storage_client.bucket(bucket_name).blob(file_name)
        content = blob.download_as_text().splitlines()
        
        # Parse header and sample rows
        reader = csv.reader(content)
        raw_header = next(reader)
        sample_rows = []
        
        # Read sample rows (but skip empty lines)
        for _ in range(sample_size):
            try:
                row = next(reader)
                if any(field.strip() for field in row):  # Skip empty rows
                    sample_rows.append(row)
            except StopIteration:
                break
        
        if not sample_rows:
            raise ValueError("No data rows found in CSV")
        
        # Transpose rows to columns for analysis
        columns = list(zip(*sample_rows))
        
        # Detect types for each column
        clean_fields = []
        schema = []
        type_report = {}
        
        for col_name, col_values in zip(raw_header, columns):
            clean_name = clean_column(col_name)
            col_type = detect_column_type(col_values)
            
            clean_fields.append(clean_name)
            schema.append(bigquery.SchemaField(clean_name, col_type))
            type_report[clean_name] = col_type
            
            logger.info(f"Detected column: {clean_name:20} | Type: {col_type}")
        
        return clean_fields, schema, type_report
        
    except Exception as e:
        logger.error(f"Error processing CSV: {str(e)}")
        return None, None, None

def parse_filename(file_name):
    """Parse filename to extract dataset, table, and mode info"""
    try:
        meta, actual_file = file_name.split("__", 1)
        dataset_id, table_id, mode = meta.split("-")
        return dataset_id, table_id, mode.lower(), actual_file
    except Exception:
        raise ValueError("Filename must be in format: dataset-table-mode__file.csv")

def verify_dataset_creation(bq, dataset_ref, max_attempts=5, delay=30):
    """Verify dataset was created successfully"""
    for attempt in range(max_attempts):
        try:
            bq.get_dataset(dataset_ref)
            logger.info(f"Verified dataset exists: {dataset_ref.dataset_id}")
            return True
        except NotFound:
            if attempt < max_attempts - 1:
                logger.info(f"Waiting for dataset creation (attempt {attempt + 1}/{max_attempts})...")
                time.sleep(delay)
            else:
                logger.error(f"Dataset not found after {max_attempts} attempts")
                return False

def verify_table_load(bq, table_ref, max_attempts=5, delay=30):
    """Verify table was loaded successfully"""
    for attempt in range(max_attempts):
        try:
            table = bq.get_table(table_ref)
            if table.num_rows > 0:
                logger.info(f"Verified table loaded with {table.num_rows} rows")
                return True
            elif attempt < max_attempts - 1:
                logger.info(f"Waiting for data load (attempt {attempt + 1}/{max_attempts})...")
                time.sleep(delay)
            else:
                logger.warning("Table created but appears to be empty")
                return True
        except NotFound:
            logger.error(f"Table not found: {table_ref}")
            return False

def create_dataset_if_not_exists(bq, project_id, dataset_id):
    """Handle dataset creation logic"""
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    try:
        bq.get_dataset(dataset_ref)
        logger.info(f"Dataset exists: {dataset_id}")
        return True
    except NotFound:
        logger.info(f"Creating dataset: {dataset_id}")
        try:
            bq.create_dataset(dataset_ref)
            logger.info("Waiting for dataset creation to complete...")
            return verify_dataset_creation(bq, dataset_ref)
        except Conflict:
            logger.warning("Dataset was already created by another process")
            return True
        except Exception as e:
            logger.error(f"Error creating dataset: {str(e)}")
            return False

def load_csv_to_bigquery(bq, uri, table_ref, schema, mode):
    """Handle the CSV loading with typed schema"""
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=(
            bigquery.WriteDisposition.WRITE_TRUNCATE if mode == 'create'
            else bigquery.WriteDisposition.WRITE_APPEND
        ),
        field_delimiter=',',
        allow_quoted_newlines=True,
        ignore_unknown_values=False,
        autodetect=False,
        # Add error handling configuration
        max_bad_records=1000,  # Allow some bad records instead of failing completely
        allow_jagged_rows=False
    )

    logger.info(f"Starting load job for {uri}")
    logger.info("Schema being applied:")
    for field in schema:
        logger.info(f"  - {field.name}: {field.field_type}")
    
    try:
        load_job = bq.load_table_from_uri(uri, table_ref, job_config=job_config)
        load_job.result()
        
        # Check for load job errors
        if load_job.errors:
            logger.warning(f"Load job completed with errors: {load_job.errors}")
        else:
            logger.info(f"Load job completed successfully for {table_ref}")
            
        return verify_table_load(bq, table_ref)
    except Exception as e:
        logger.error(f"Load job failed: {str(e)}")
        return False

def run_data_quality_checks(bq, table_ref, clean_fields):
    """Run essential quality checks without cleaning"""
    validator = DataValidator(bq)
    
    # Check 1: Table exists and has data
    row_count = validator.get_row_count(table_ref)
    if row_count == 0:
        logger.error("Empty dataset - cannot train model")
        return False
    
    # Check 2: Basic data integrity
    if not validator.validate_data_integrity(table_ref, clean_fields):
        logger.warning("Data integrity validation failed")
        return False
    
    # Check 3: At least 2 columns (features + target)
    if len(clean_fields) < 2:
        logger.error("Insufficient columns for training")
        return False
    
    return True

def gcs_to_bq(event, context):
    """Main Cloud Function entry point"""
    logger.info(f"Triggered by: {event['bucket']}/{event['name']}")
    
    # Skip non-CSV files
    if not event['name'].endswith('.csv'):
        logger.info(f"Skipping non-CSV file: {event['name']}")
        return

    try:
        # Parse filename metadata
        dataset_id, table_id, mode, actual_file = parse_filename(event['name'])
        logger.info(f"Parsed filename: dataset={dataset_id}, table={table_id}, mode={mode}")

        # Initialize clients
        bq = bigquery.Client()
        storage_client = storage.Client()
        project_id = bq.project
        bucket_name = event['bucket']
        logger.info(f"Operating in project: {project_id}")
        
        # Process CSV schema with type detection
        clean_fields, schema, type_report = process_csv_schema(
            storage_client, 
            bucket_name, 
            event['name']
        )
        if not clean_fields:
            logger.error("Failed to process schema - aborting")
            return

        # Create dataset if needed
        if not create_dataset_if_not_exists(bq, project_id, dataset_id):
            return

        # Check if table exists for 'create' mode
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        if mode == 'create':
            try:
                existing_table = bq.get_table(table_ref)
                logger.warning(f"Table already exists: {table_ref} ({existing_table.num_rows} rows)")
                return
            except NotFound:
                pass

        # Load data into BigQuery
        uri = f"gs://{bucket_name}/{event['name']}"
        if not load_csv_to_bigquery(bq, uri, table_ref, schema, mode):
            return

        # Run essential quality checks
        if not run_data_quality_checks(bq, table_ref, clean_fields):
            logger.error("Dataset validation failed")
            logger.info(f"Processing complete for {event['name']}")
            return

        logger.info(f"Data successfully loaded to BigQuery table: {table_ref}")
        logger.info(f"Table contains {len(clean_fields)} columns and passed validation checks")
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}", exc_info=True)
        raise
    finally:
        logger.info(f"Processing complete for {event['name']}")