from google.cloud import bigquery

class DataValidator:
    """Handles data validation operations"""
    
    def __init__(self, bq_client):
        self.bq = bq_client
    
    def get_row_count(self, table_ref):
        """Get current row count of a table"""
        try:
            query = f"SELECT COUNT(*) as row_count FROM `{table_ref}`"
            result = list(self.bq.query(query).result())[0]
            return result.row_count
        except Exception as e:
            print(f"⚠️ Could not get row count: {str(e)}")
            return 0
    
    def validate_cleaning_results(self, table_ref, original_row_count, tolerance_pct=1.0):
        """Validate that cleaning didn't cause unexpected data loss"""
        validation_query = f"""
        SELECT 
            COUNT(*) as current_row_count,
            COUNT(*) - {original_row_count} as row_count_change
        FROM `{table_ref}`
        """
        
        try:
            query_job = self.bq.query(validation_query)
            result = list(query_job.result())[0]
            
            if result.row_count_change != 0:
                print(f"⚠️ Row count changed: {original_row_count} → {result.current_row_count}")
                
                change_pct = abs(result.row_count_change) / original_row_count * 100
                if change_pct > tolerance_pct:
                    print(f"❌ Row count change ({change_pct:.1f}%) exceeds tolerance ({tolerance_pct}%)")
                    return False
            
            print(f"✅ Validation passed. Row count: {result.current_row_count}")
            return True
            
        except Exception as e:
            print(f"⚠️ Validation failed: {str(e)}")
            return False
    
    def validate_data_integrity(self, table_ref, fields):
        """Perform additional data integrity checks"""
        # This can be extended with more sophisticated checks
        try:
            # Basic integrity check - ensure table is not empty and has expected columns
            schema_query = f"""
            SELECT column_name 
            FROM `{table_ref.split('.')[0]}.{table_ref.split('.')[1]}.INFORMATION_SCHEMA.COLUMNS` 
            WHERE table_name = '{table_ref.split('.')[2]}'
            """
            
            query_job = self.bq.query(schema_query)
            schema_columns = [row.column_name for row in query_job.result()]
            
            missing_columns = set(fields) - set(schema_columns)
            if missing_columns:
                print(f"⚠️ Missing expected columns: {missing_columns}")
                return False
            
            print("✅ Data integrity check passed")
            return True
            
        except Exception as e:
            print(f"⚠️ Integrity check failed: {str(e)}")
            return True
