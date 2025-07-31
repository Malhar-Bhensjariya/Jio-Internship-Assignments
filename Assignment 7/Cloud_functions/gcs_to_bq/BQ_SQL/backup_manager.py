import time
from google.cloud import bigquery

class BackupManager:
    """Handles backup creation and restoration for BigQuery tables"""
    
    def __init__(self, bq_client):
        self.bq = bq_client
    
    def create_backup(self, table_ref):
        """Create a backup of the specified table"""
        backup_table_ref = f"{table_ref}_backup_{int(time.time())}"
        
        backup_query = f"""
        CREATE TABLE `{backup_table_ref}` AS 
        SELECT * FROM `{table_ref}`
        """
        
        try:
            query_job = self.bq.query(backup_query)
            query_job.result()
            print(f"💾 Backup created: {backup_table_ref}")
            return backup_table_ref
        except Exception as e:
            print(f"⚠️ Backup creation failed: {str(e)}")
            return None
    
    def restore_from_backup(self, table_ref, backup_ref):
        """Restore table from backup"""
        restore_query = f"""
        CREATE OR REPLACE TABLE `{table_ref}` AS 
        SELECT * FROM `{backup_ref}`
        """
        
        try:
            query_job = self.bq.query(restore_query)
            query_job.result()
            print(f"↩️ Restored from backup: {backup_ref}")
            return True
        except Exception as e:
            print(f"❌ Restore failed: {str(e)}")
            return False
    
    def cleanup_backup(self, backup_ref):
        """Clean up backup table"""
        try:
            self.bq.delete_table(backup_ref)
            print(f"🗑️ Backup cleaned up: {backup_ref}")
            return True
        except Exception as e:
            print(f"⚠️ Could not clean up backup {backup_ref}: {str(e)}")
            return False
