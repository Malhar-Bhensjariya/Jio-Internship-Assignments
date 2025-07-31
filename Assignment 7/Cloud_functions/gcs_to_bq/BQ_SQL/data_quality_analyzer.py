from google.cloud import bigquery
import logging

class DataQualityAnalyzer:
    def __init__(self, bq_client):
        self.client = bq_client
    
    def analyze_table_quality(self, table_ref, columns):
        """Generate comprehensive data quality report"""
        try:
            # Basic validation
            if not isinstance(table_ref, str) or table_ref.count('.') != 2:
                raise ValueError(f"Invalid table reference: {table_ref}")
            
            # Get table metadata
            table = self.client.get_table(table_ref)
            num_rows = table.num_rows
            if num_rows == 0:
                logging.warning("Empty table - skipping quality analysis")
                return None

            # Generate quality metrics
            quality_report = {
                'table': table_ref,
                'total_rows': num_rows,
                'columns': {}
            }

            for col in columns:
                col_analysis = self._analyze_column(table_ref, col)
                quality_report['columns'][col] = col_analysis
            
            return quality_report
            
        except Exception as e:
            logging.error(f"Quality analysis failed: {str(e)}")
            return None
    
    def _analyze_column(self, table_ref, column_name):
        """Analyze quality for a single column"""
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(`{column_name}`) as non_null_count,
            COUNTIF(TRIM(`{column_name}`) = '') as empty_string_count,
            COUNTIF(REGEXP_CONTAINS(`{column_name}`, r'^\\s+$')) as whitespace_only_count,
            APPROX_COUNT_DISTINCT(`{column_name}`) as distinct_values,
            MIN(LENGTH(`{column_name}`)) as min_length,
            MAX(LENGTH(`{column_name}`)) as max_length
        FROM `{table_ref}`
        """
        
        try:
            query_job = self.client.query(query)
            results = list(query_job.result())[0]
            
            return {
                'non_null_pct': (results.non_null_count / results.total_rows) * 100,
                'empty_pct': (results.empty_string_count / results.total_rows) * 100,
                'whitespace_pct': (results.whitespace_only_count / results.total_rows) * 100,
                'distinct_values': results.distinct_values,
                'length_range': (results.min_length, results.max_length)
            }
        except Exception as e:
            logging.error(f"Column analysis failed for {column_name}: {str(e)}")
            return None