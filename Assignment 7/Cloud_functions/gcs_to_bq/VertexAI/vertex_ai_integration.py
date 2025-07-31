from google.cloud import aiplatform, bigquery
from datetime import datetime
import time

class VertexAITrainer:
    def __init__(self, project_id, location="asia-south1"):
        aiplatform.init(project=project_id, location=location)
        self.project_id = project_id
        self.location = location

    def _determine_task_type(self, target_column_stats):
        """Determine if classification or regression"""
        print("[DEBUG] Determining task type based on target column stats")
        if target_column_stats['has_non_numeric'] or target_column_stats['distinct_values'] <= 50:
            print("[DEBUG] Task type identified as: classification")
            return "classification"
        print("[DEBUG] Task type identified as: regression")
        return "regression"

    def _get_column_stats(self, dataset_id, table_id, target_column):
        """Get statistics about the target column"""
        print(f"[DEBUG] Getting column stats for {target_column}")
        client = bigquery.Client(project=self.project_id)
        query = f"""
        SELECT 
            APPROX_COUNT_DISTINCT(`{target_column}`) as distinct_values,
            LOGICAL_OR(REGEXP_CONTAINS(CAST(`{target_column}` AS STRING), r'[^0-9\.]')) as has_non_numeric
        FROM `{self.project_id}.{dataset_id}.{table_id}`
        """
        result = list(client.query(query).result())[0]
        print(f"[DEBUG] Column stats retrieved: {result}")
        return result

    def trigger_automl_training(self, dataset_id, table_id, target_column, column_types=None):
        """Trigger AutoML training with proper checkpointing and error handling"""
        try:
            # Checkpoint 1: Dataset creation
            print(f"[CHECKPOINT 1] Creating Vertex AI Dataset from BQ table {dataset_id}.{table_id}")
            dataset = aiplatform.TabularDataset.create(
                display_name=f"{table_id}_dataset_{datetime.now().strftime('%Y%m%d')}",
                bq_source=f"bq://{self.project_id}.{dataset_id}.{table_id}"
            )
            print(f"[SUCCESS] Created Vertex AI Dataset: {dataset.resource_name}")

            # Checkpoint 2: Column transformations
            print("[CHECKPOINT 2] Preparing column transformations")
            transformations = []
            if column_types:
                for col, col_type in column_types.items():
                    if col == target_column:
                        continue  # Skip target column
                    
                    if col_type == "BOOLEAN":
                        transformations.append({"categorical": {"column_name": col}})
                    elif col_type in ["INT64", "FLOAT64"]:
                        transformations.append({"numeric": {"column_name": col}})
                    else:
                        transformations.append({"categorical": {"column_name": col}})
            
            print(f"[DEBUG] Transformations prepared: {transformations}")

            # Checkpoint 3: Get target column stats
            print("[CHECKPOINT 3] Analyzing target column statistics")
            target_stats = self._get_column_stats(dataset_id, table_id, target_column)
            task_type = self._determine_task_type(target_stats)
            print(f"[DEBUG] Determined task type: {task_type}")

            # Checkpoint 4: Training job initialization
            print("[CHECKPOINT 4] Initializing AutoML training job")
            job = aiplatform.AutoMLTabularTrainingJob(
                display_name=f"{table_id}_training_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                optimization_prediction_type=task_type,
                column_transformations=transformations if transformations else None
            )
            print("[SUCCESS] Training job initialized")

            # Checkpoint 5: Starting training
            print("[CHECKPOINT 5] Starting model training")
            print(f"[DEBUG] Training parameters: target={target_column}, "
                  f"train_split=0.8, val_split=0.1, test_split=0.1, budget=1000")
            
            model = job.run(
                dataset=dataset,
                target_column=target_column,
                training_fraction_split=0.8,
                validation_fraction_split=0.1,
                test_fraction_split=0.1,
                budget_milli_node_hours=1000,
                disable_early_stopping=False,
                sync=True
            )

            print(f"[SUCCESS] Training completed. Model: {model.resource_name}")
            return model

        except Exception as e:
            print(f"[ERROR] Vertex AI Training failed at checkpoint")
            print(f"[ERROR DETAILS] {str(e)}")
            raise