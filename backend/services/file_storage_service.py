import os
import shutil
import logging
from fastapi import UploadFile

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class FileStorageService:
    """
    Handles file storage to a persistent disk in the cloud environment.
    """
    def __init__(self):
        # Render provides a persistent disk at /var/data
        self.base_dir = "/var/data"
        self.upload_dir = os.path.join(self.base_dir, "uploads")
        self.results_dir = os.path.join(self.base_dir, "results")
        
        os.makedirs(self.upload_dir, exist_ok=True)
        os.makedirs(self.results_dir, exist_ok=True)
        
    def save_file(self, file: UploadFile, workflow_id: str) -> str:
        """
        Saves an uploaded file to the persistent disk.
        
        Args:
            file: The file to be saved.
            workflow_id: The ID of the workflow to group files.
            
        Returns:
            The file path where the file was saved.
        """
        workflow_path = os.path.join(self.upload_dir, workflow_id)
        os.makedirs(workflow_path, exist_ok=True)
        
        file_path = os.path.join(workflow_path, file.filename)
        try:
            with open(file_path, "wb") as buffer:
                shutil.copyfileobj(file.file, buffer)
            return file_path
        except Exception as e:
            logging.error(f"Failed to save file {file.filename}: {e}")
            raise RuntimeError(f"Failed to save file: {e}")

    def save_results(self, workflow_id: str, results_data: dict):
        """
        Saves the processed results as a JSON file.
        """
        results_file = os.path.join(self.results_dir, f'{workflow_id}.json')
        try:
            with open(results_file, 'w') as f:
                json.dump(results_data, f, indent=2)
            return results_file
        except Exception as e:
            logging.error(f"Failed to save results for workflow {workflow_id}: {e}")
            raise RuntimeError(f"Failed to save results: {e}")

    def get_results(self, workflow_id: str):
        """
        Retrieves a saved results file.
        """
        results_file = os.path.join(self.results_dir, f'{workflow_id}.json')
        if not os.path.exists(results_file):
            raise FileNotFoundError("Results file not found")
        
        try:
            with open(results_file, 'r') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Failed to read results file for workflow {workflow_id}: {e}")
            raise RuntimeError(f"Failed to read results: {e}")