import pandas as pd
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataIngestionPipeline:
    def __init__(self):
        # MongoDB connection
        self.mongo_uri = f"mongodb://{os.getenv('MONGO_USERNAME', 'root')}:{os.getenv('MONGO_PASSWORD', 'example')}@mongodb:27017/"
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client['ecommerce_db']
        self.collection = self.db['raw_data']

    def read_data(self, file_path):
        """Read data from CSV file"""
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"CSV file not found at {file_path}. Current directory contains: {os.listdir('.')}")
            
            logger.info(f"Reading data from {file_path}")
            return pd.read_csv(file_path)
        except Exception as e:
            logger.error(f"Error reading data: {str(e)}")
            raise

    def preprocess_data(self, df):
        """Basic preprocessing before ingestion"""
        try:
            # Add ingestion timestamp
            df['ingestion_timestamp'] = datetime.now()
            
            # Convert DataFrame to dictionary format for MongoDB
            return df.to_dict('records')
        except Exception as e:
            logger.error(f"Error preprocessing data: {str(e)}")
            raise

    def store_in_mongodb(self, data):
        """Store data in MongoDB"""
        try:
            # Clear existing data
            self.collection.delete_many({})
            logger.info("Cleared existing data from MongoDB")
            
            # Store new data
            logger.info("Storing data in MongoDB")
            result = self.collection.insert_many(data)
            logger.info(f"Successfully inserted {len(result.inserted_ids)} documents")
            return result.inserted_ids
        except Exception as e:
            logger.error(f"Error storing data in MongoDB: {str(e)}")
            raise

    def run_pipeline(self):
        """Execute the complete data ingestion pipeline"""
        try:
            logger.info(f"Current working directory: {os.getcwd()}")
            logger.info(f"Directory contents: {os.listdir('.')}")
            
            # Define the file path
            file_path = 'data/ecommerce_data_with_trends.csv'
            
            # Read data
            df = self.read_data(file_path)
            logger.info(f"Read {len(df)} records from CSV")

            # Preprocess data
            processed_data = self.preprocess_data(df)
            logger.info("Data preprocessing completed")

            # Store in MongoDB
            inserted_ids = self.store_in_mongodb(processed_data)
            logger.info("Data successfully stored in MongoDB")

            return {
                'status': 'success',
                'records_processed': len(df),
                'records_stored': len(inserted_ids)
            }

        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e)
            }

    def verify_data(self):
        """Verify data in MongoDB"""
        try:
            count = self.collection.count_documents({})
            logger.info(f"Total documents in MongoDB: {count}")
            return count
        except Exception as e:
            logger.error(f"Error verifying data: {str(e)}")
            raise

if __name__ == "__main__":
    # Initialize and run pipeline
    pipeline = DataIngestionPipeline()
    result = pipeline.run_pipeline()
    
    # Print results
    if result['status'] == 'success':
        print(f"Successfully processed {result['records_processed']} records")
        print(f"Data stored in MongoDB: {pipeline.verify_data()} documents")
    else:
        print(f"Pipeline failed: {result['error']}")
