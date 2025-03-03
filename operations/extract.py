"""Data extraction operations."""

import logging
import os
from pathlib import Path

import pandas as pd

from ..config.config import config
from ..database.connection import OracleConnection
from ..database.queries import QueryBuilder

logger = logging.getLogger(__name__)


class DataExtractor:
    """Class for extracting data from various sources."""
    
    @staticmethod
    def extract_from_database(query=None, params=None):
        """Extract data from the Oracle database.
        
        Args:
            query: SQL query (if None, will use default forecast query)
            params: Parameters for the query
            
        Returns:
            DataFrame: Extracted data
        """
        logger.info("Extracting data from Oracle database")
        
        # Use default forecast query if none provided
        if query is None:
            query = QueryBuilder.get_forecast_query()
            
        # Connect to database and execute query
        with OracleConnection() as conn:
            df = conn.query_to_dataframe(query, params)
            
        logger.info(f"Extracted {df.shape[0]} rows from database")
        return df
    
    @staticmethod
    def extract_from_csv(file_path, sep=",", encoding="utf-8", **kwargs):
        """Extract data from CSV file.
        
        Args:
            file_path: Path to CSV file
            sep: Column separator (default: comma)
            encoding: File encoding (default: utf-8)
            **kwargs: Additional arguments for pd.read_csv
            
        Returns:
            DataFrame: Extracted data
        """
        logger.info(f"Extracting data from CSV file: {file_path}")
        
        try:
            df = pd.read_csv(file_path, sep=sep, encoding=encoding, **kwargs)
            logger.info(f"Extracted {df.shape[0]} rows from CSV file")
            return df
            
        except Exception as e:
            logger.exception(f"Error extracting data from CSV file: {file_path}")
            raise ValueError(f"Failed to extract data from CSV: {str(e)}")
    
    @staticmethod
    def extract_from_excel(file_path, sheet_name=0, **kwargs):
        """Extract data from Excel file.
        
        Args:
            file_path: Path to Excel file
            sheet_name: Sheet name or index (default: 0)
            **kwargs: Additional arguments for pd.read_excel
            
        Returns:
            DataFrame: Extracted data
        """
        logger.info(f"Extracting data from Excel file: {file_path}")
        
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
            logger.info(f"Extracted {df.shape[0]} rows from Excel file")
            return df
            
        except Exception as e:
            logger.exception(f"Error extracting data from Excel file: {file_path}")
            raise ValueError(f"Failed to extract data from Excel: {str(e)}")
    
    @staticmethod
    def extract_from_tab_delimited(file_path, encoding="utf-8", **kwargs):
        """Extract data from tab-delimited file.
        
        Args:
            file_path: Path to tab-delimited file
            encoding: File encoding (default: utf-8)
            **kwargs: Additional arguments for pd.read_csv
            
        Returns:
            DataFrame: Extracted data
        """
        logger.info(f"Extracting data from tab-delimited file: {file_path}")
        
        try:
            df = pd.read_csv(file_path, sep="\t", encoding=encoding, **kwargs)
            logger.info(f"Extracted {df.shape[0]} rows from tab-delimited file")
            return df
            
        except Exception as e:
            logger.exception(f"Error extracting data from tab-delimited file: {file_path}")
            raise ValueError(f"Failed to extract data from tab-delimited file: {str(e)}")
    
    @staticmethod
    def list_files_in_directory(directory="."):
        """List all files in the specified directory.
        
        Args:
            directory: Directory path (default: current directory)
            
        Returns:
            dict: Dictionary of {index: filename}
        """
        files = {}
        i = 1
        
        try:
            for file in sorted(os.listdir(directory)):
                if os.path.isfile(os.path.join(directory, file)):
                    files[i] = file
                    i += 1
                    
            return files
            
        except Exception as e:
            logger.exception(f"Error listing files in directory: {directory}")
            raise ValueError(f"Failed to list files: {str(e)}")
