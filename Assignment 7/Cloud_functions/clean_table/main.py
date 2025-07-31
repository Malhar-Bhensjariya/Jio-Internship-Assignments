import base64
import json
from google.cloud import bigquery
from datetime import datetime

def clean_bq_table(event, context):
    # Step 1: Decode Pub/Sub message
    payload = json.loads(base64.b64decode(event['data']).decode('utf-8'))
    project_id = payload['project_id']
    dataset_id = payload['dataset_id']
    table_id = payload['table_id']

    client = bigquery.Client(project=project_id)
    source_table = f"{project_id}.{dataset_id}.{table_id}"
    
    print(f"Cleaning table: {source_table}")

    try:
        # Get table info
        table_ref = client.get_table(source_table)
        total_rows = table_ref.num_rows
        print(f"Total rows: {total_rows}")
        
        if total_rows == 0:
            print("⚠️ Table is empty, skipping cleaning")
            return

        # Count rows with NAs using SQL
        null_conditions = [f"{field.name} IS NULL" for field in table_ref.schema]
        na_query = f"""
        SELECT COUNT(*) as na_count
        FROM `{source_table}`
        WHERE {' OR '.join(null_conditions)}
        """
        
        na_count = client.query(na_query).to_dataframe().iloc[0]['na_count']
        na_percentage = na_count / total_rows
        print(f"Rows with NAs: {na_count} ({na_percentage:.1%})")

        # Count duplicates using SQL
        dup_query = f"""
        SELECT COUNT(*) - COUNT(DISTINCT TO_JSON_STRING(t)) as dup_count
        FROM `{source_table}` t
        """
        
        dup_count = client.query(dup_query).to_dataframe().iloc[0]['dup_count']
        dup_percentage = dup_count / total_rows
        print(f"Duplicate rows: {dup_count} ({dup_percentage:.1%})")

        # Build cleaning query
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        new_table_id = f"{table_id}_cleaned_{timestamp}"
        destination = f"{project_id}.{dataset_id}.{new_table_id}"

        # Start with base query
        select_clause = "SELECT *"
        from_clause = f"FROM `{source_table}`"
        where_clause = ""

        # Add DISTINCT if duplicates < 10%
        if dup_percentage < 0.1 and dup_count > 0:
            select_clause = "SELECT DISTINCT *"
            print("✅ Will remove duplicates")
        else:
            print("⚠️ Keeping duplicates (too many or none found)")

        # Add WHERE clause for NAs if < 10%
        if na_percentage < 0.1 and na_count > 0:
            not_null_conditions = [f"{field.name} IS NOT NULL" for field in table_ref.schema]
            where_clause = f"WHERE {' AND '.join(not_null_conditions)}"
            print("✅ Will remove NA rows")
        else:
            print("⚠️ Keeping NA rows (too many or none found)")

        # Combine query parts
        cleaning_query = f"{select_clause} {from_clause} {where_clause}"
        
        print(f"Executing cleaning query...")
        print(f"Query: {cleaning_query}")

        # Execute cleaning query
        job_config = bigquery.QueryJobConfig(destination=destination)
        job = client.query(cleaning_query, job_config=job_config)
        job.result()  # Wait for completion

        # Get final row count
        final_table = client.get_table(destination)
        final_rows = final_table.num_rows
        removed_rows = total_rows - final_rows
        
        print(f"✅ Cleaned table created: {destination}")
        print(f"✅ Removed {removed_rows} rows ({removed_rows/total_rows:.1%})")
        print(f"✅ Final table has {final_rows} rows")

    except Exception as e:
        print(f"❌ Error cleaning table: {str(e)}")
        raise e