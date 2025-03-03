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
            "Validity_Date":        "validity_
