import os
import urllib.request as request
import zipfile
from multimodalsentimentanalysis import logger
from multimodalsentimentanalysis.utils.common import get_size
from multimodalsentimentanalysis.entity.config_entity import DataIngestionConfig
from pathlib import Path
import gdown


class DataIngestion:
    def __init__(self, config: DataIngestionConfig):
        self.config = config

    def download_file(self):
        if not os.path.exists(self.config.local_data_file):
            try:
                # Download the file using gdown
                gdown.download(self.config.source_URL, str(self.config.local_data_file), quiet=False)
                
                # Check if the downloaded file is a ZIP file
                if not self._is_zip_file(Path(self.config.local_data_file)):
                    logger.error(f"Downloaded file: {self.config.local_data_file}")
                    raise ValueError("Downloaded file is not a ZIP file.")
                
            except Exception as e:
                logger.error(f"Error downloading the file: {e}")
                raise
        else:
            logger.info(f"File already exists of size: {get_size(Path(self.config.local_data_file))}")

    def _is_zip_file(self, file_path: Path) -> bool:
        """Check if the given file is a ZIP file."""
        try:
            with open(file_path, 'rb') as f:
                header = f.read(4)
            # ZIP files start with the bytes 'PK\x03\x04'
            return header == b'PK\x03\x04'
        except Exception as e:
            logger.error(f"Error checking if file is ZIP: {e}")
            return False

    def extract_zip_file(self):
        """Extracts the zip file into the specified directory."""
        unzip_path = self.config.unzip_dir
        os.makedirs(unzip_path, exist_ok=True)
        try:
            with zipfile.ZipFile(self.config.local_data_file, 'r') as zip_ref:
                zip_ref.extractall(unzip_path)
            logger.info(f"Extracted the files to {unzip_path}")
            
            # List extracted files
            for file_name in os.listdir(unzip_path):
                if file_name.endswith('.csv'):
                    logger.info(f"CSV file found: {file_name}")
        except zipfile.BadZipFile as e:
            logger.error(f"Error extracting the zip file: {e}")
            raise
