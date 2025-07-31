from google.cloud import bigquery

class DataCleaner:
    """Handles intelligent data cleaning operations"""
    
    def __init__(self, bq_client):
        self.bq = bq_client
    
    def perform_smart_cleaning(self, table_ref, fields, quality_report, config=None):
        """Perform intelligent cleaning based on quality analysis"""
        if not config:
            config = self._get_default_config()
        
        print(f"🧹 Analyzing cleaning requirements...")
        
        cleaning_operations = self._determine_cleaning_operations(fields, quality_report, config)
        
        if not cleaning_operations:
            print("✅ No cleaning needed - data quality is acceptable!")
            return True
        
        return self._execute_cleaning_operations(table_ref, cleaning_operations)
    
    def _get_default_config(self):
        """Get default cleaning configuration"""
        return {
            'max_empty_percentage': 50,
            'min_issue_percentage': 1,
            'operations': ['trim', 'nullify_empty', 'nullify_whitespace']
        }
    
    def _determine_cleaning_operations(self, fields, quality_report, config):
        """Determine which cleaning operations to perform"""
        cleaning_operations = []
        
        for field in fields:
            stats = quality_report.get(field, {})
            total_rows = stats.get('total_rows', 0)
            
            if total_rows == 0:
                continue
            
            empty_pct = (stats.get('empty_string_count', 0) / total_rows) * 100
            whitespace_pct = (stats.get('whitespace_only_count', 0) / total_rows) * 100
            total_issues_pct = empty_pct + whitespace_pct
            
            # Only clean if issues are within acceptable bounds
            if (config['min_issue_percentage'] <= total_issues_pct <= config['max_empty_percentage']):
                operations = []
                
                if 'trim' in config['operations']:
                    operations.append('TRIM')
                
                if 'nullify_empty' in config['operations'] and empty_pct < config['max_empty_percentage']:
                    operations.append('NULLIF_EMPTY')
                
                if 'nullify_whitespace' in config['operations'] and whitespace_pct < config['max_empty_percentage']:
                    operations.append('NULLIF_WHITESPACE')
                
                if operations:
                    cleaning_operations.append({
                        'field': field,
                        'operations': operations,
                        'issues_pct': total_issues_pct
                    })
        
        return cleaning_operations
    
    def _execute_cleaning_operations(self, table_ref, cleaning_operations):
        """Execute the determined cleaning operations"""
        print(f"🔧 Executing cleaning on {len(cleaning_operations)} columns...")
        
        update_clauses = []
        for op in cleaning_operations:
            field = op['field']
            operations = op['operations']
            
            field_expr = f"`{field}`"
            
            # Build nested operations
            if 'TRIM' in operations:
                field_expr = f"TRIM({field_expr})"
            
            if 'NULLIF_EMPTY' in operations:
                field_expr = f"NULLIF({field_expr}, '')"
            
            if 'NULLIF_WHITESPACE' in operations:
                field_expr = f"CASE WHEN REGEXP_CONTAINS({field_expr}, r'^\\s+$') THEN NULL ELSE {field_expr} END"
            
            update_clauses.append(f"`{field}` = {field_expr}")
            print(f"  → {field}: {', '.join(operations)} (affects ~{op['issues_pct']:.1f}% of data)")
        
        # Execute cleaning query
        cleaning_query = f"""
        UPDATE `{table_ref}`
        SET {', '.join(update_clauses)}
        WHERE true
        """
        
        try:
            query_job = self.bq.query(cleaning_query)
            query_job.result()
            rows_affected = query_job.num_dml_affected_rows
            print(f"✅ Cleaning complete. {rows_affected} rows processed")
            return True
        except Exception as e:
            print(f"❌ Cleaning execution failed: {str(e)}")
            return False