"""Upload data from file to Oracle database."""

import logging
import os
import traceback
from datetime import datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import MetaData, Table

from ..config.config import config
from ..database.connection import OracleConnection
from ..utils.credentials import credential_manager

logger = logging.getLogger(__name__)


def list_files():
    """List files in the current directory.
    
    Returns:
        dict: Dictionary of {index: filename}
    """
    # Get all files in current directory
    files = {}
    i = 1
    
    for file in sorted(os.listdir('.')):
        if os.path.isfile(file):
            files[i] = file
            i += 1
    
    return files


def prompt_for_file_selection():
    """Prompt user to select a file.
    
    Returns:
        str: Selected filename
    """
    files = list_files()
    
    # Show list of files
    print("\nHere is a list of files in the current directory:")
    for no, file in files.items():
        print(f"{no}. {file}")
    
    # Prompt for selection
    while True:
        try:
            selection = input("\nEnter the number of the file to upload: ")
            selection = int(selection)
            
            if selection in files:
                return files[selection]
            else:
                print("Invalid selection. Please try again.")
        except ValueError:
            print("Please enter a number.")


def load_file_to_dataframe(filename):
    """Load file data into a DataFrame.
    
    Args:
        filename: Path to file
        
    Returns:
        DataFrame: Loaded data
    """
    logger.info(f"Loading data from file: {filename}")
    
    try:
        # Load the file
        df = pd.read_csv(filename, sep="\t")
        
        # Rename columns
        renamed_columns = {
            "Validity_Date":        "validity_date",
            "Timezone":             "timezone",
            "Part_ID":              "part_id",
            "Product_ID":           "product_id",
            "Product_Description":  "product_description",
            "Last_Submitted_Date":  "last_submitted_date",
            "Location_ID":          "location_id",
            "Key_Figure":           "key_figure",
            "Planned_Month":        "planned_month",
            "FINAL_CON_DEM":        "qty"
        }
        df = df.rename(columns=renamed_columns)
        
        # Reorder columns
        df = df[[
            'validity_date',
            'timezone',
            'part_id',
            'product_id',
            'product_description',
            'last_submitted_date',
            'location_id',
            'key_figure',
            'planned_month',
            'qty'
        ]]
        
        logger.info(f"Loaded {df.shape[0]} rows from {filename}")
        return df
        
    except Exception as e:
        logger.exception(f"Error loading file: {filename}")
        raise ValueError(f"Failed to load file: {str(e)}")


def upload_to_database(engine, df, tablename):
    """Upload DataFrame to database table.
    
    Args:
        engine: SQLAlchemy engine
        df: DataFrame to upload
        tablename: Target table name
    """
    logger.info(f"Uploading data to table: {tablename}")
    
    try:
        # Check if the data is already in the table
        # Get validity_date from the file
        file_date_str = df["validity_date"].iloc[0]
        file_date = datetime.strptime(file_date_str, "%Y.%m.%d %H:%M:%S")
        
        # Get max date from the table
        query = f"SELECT MAX(validity_date) as max_date FROM {tablename}"
        result = pd.read_sql(query, engine)
        max_table_date = result["max_date"].iloc[0]
        
        if max_table_date is not None:
            max_table_date = pd.to_datetime(max_table_date)
            
            if file_date <= max_table_date:
                logger.info("Data already exists in the table (same or older date)")
                print("Data is already updated in the table.")
                return
        
        # Create metadata and table reference
        metadata = MetaData()
        metadata.reflect(bind=engine, only=[tablename])
        
        if tablename not in metadata.tables:
            raise ValueError(f"Table {tablename} does not exist in the database")
            
        table = metadata.tables[tablename]
        
        # Convert DataFrame to records
        records = df.to_dict(orient='records')
        
        # Insert data
        with engine.begin() as connection:
            connection.execute(table.insert(), records)
            
        logger.info(f"Successfully inserted {len(records)} records into {tablename}")
        print(f"Successfully inserted {len(records)} records into {tablename}")
        
    except Exception as e:
        logger.exception(f"Error uploading to table: {tablename}")
        raise ValueError(f"Failed to upload data: {str(e)}")


def upload_data(filename=None, tablename="t_ibp_cons_rdc"):
    """Upload data from file to database.
    
    Args:
        filename: Input file (if None, will prompt for selection)
        tablename: Target table name
    """
    try:
        # Print welcome message
        print("""
------------------------------------------------------------------------------------
                       Upload Data to Oracle Database
------------------------------------------------------------------------------------
This script will upload data from a file to the specified Oracle database table.
------------------------------------------------------------------------------------
        """)
        
        # Get the filename if not provided
        if filename is None:
            filename = prompt_for_file_selection()
            
        print(f"\nSelected file: {filename}")
        
        # Load data from file
        df = load_file_to_dataframe(filename)
        
        # Create database connection
        with OracleConnection() as db_conn:
            engine = db_conn.create_sqlalchemy_engine()
            
            # Upload to database
            upload_to_database(engine, df, tablename)
            
        print("\nOperation completed successfully!")
        
    except Exception as e:
        logger.exception("Error in upload_data")
        print(f"Error: {str(e)}")
        print("See log file for details.")
