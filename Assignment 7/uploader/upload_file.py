import os
from google.cloud import storage, bigquery

def list_buckets():
    storage_client = storage.Client()
    print("\nAvailable GCS Buckets:")
    for bucket in storage_client.list_buckets():
        print(f" - {bucket.name}")

def list_datasets(bq_client):
    print("\nAvailable BigQuery Datasets:")
    datasets = list(bq_client.list_datasets())
    if not datasets:
        print(" (No datasets found)")
    for ds in datasets:
        print(f" - {ds.dataset_id}")

def list_tables(bq_client, dataset_id):
    print(f"\nTables in dataset '{dataset_id}':")
    try:
        tables = list(bq_client.list_tables(dataset_id))
        if not tables:
            print(" (No tables found)")
        for tbl in tables:
            print(f" - {tbl.table_id}")
    except Exception as e:
        print(f"⚠️ Could not list tables for dataset '{dataset_id}': {e}")

def upload_csv():
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    list_buckets()
    bucket_name = input("\nEnter your GCS bucket name: ").strip()

    local_file = input("Enter the local CSV path: ").strip()
    if not os.path.exists(local_file):
        print(f"❌ File does not exist: {local_file}")
        return

    list_datasets(bq_client)
    dataset = input("\nEnter BigQuery dataset name: ").strip()

    list_tables(bq_client, dataset)
    table = input("\nEnter BigQuery table name: ").strip()

    mode = input("Mode? (create / append): ").strip().lower()
    if mode not in ['create', 'append']:
        print("❌ Mode must be 'create' or 'append'")
        return

    actual_file = os.path.basename(local_file)
    trigger_filename = f"{dataset}-{table}-{mode}__{actual_file}"

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(trigger_filename)

    blob.upload_from_filename(local_file)
    print(f"\n✅ Uploaded as: {trigger_filename} to bucket: {bucket_name}")
    print("✅ This will now trigger the Cloud Function.")

if __name__ == "__main__":
    upload_csv()
