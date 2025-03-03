"""Download BOM EO Forecast data and export to Excel/Parquet."""

import logging
import os
import time
import webbrowser
from datetime import date, datetime
from pathlib import Path

import dask.dataframe as da
import pandas as pd
from dateutil import relativedelta

from ..config.config import config
from ..database.connection import OracleConnection
from ..utils.credentials import credential_manager

logger = logging.getLogger(__name__)


def get_forecast_data():
    """Retrieve BOM EO Forecast data from Oracle database.
    
    Returns:
        tuple: (DataFrame, validity_date, forecast_cycle)
    """
    logger.info("Retrieving BOM EO Forecast data from Oracle database")
    
    # Get SQL query from config
    sql = config.get_query('bomfc_dpv_detail_hist')
    if not sql:
        raise ValueError("SQL query 'bomfc_dpv_detail_hist' not found in configuration")
    
    # Connect to the database and execute query
    with OracleConnection() as conn:
        df = conn.query_to_dataframe(sql)
    
    # Process data
    logger.info("Processing retrieved data")
    
    # Remove encoding errors
    df = df.applymap(lambda x: x.encode("unicode_escape").decode("utf-8") if isinstance(x, str) else x)
    
    # Get validity_date and forecast_cycle variables
    validity_date = pd.to_datetime(df["VALIDITY_DATE"]).dt.date.unique()
    forecast_cycle = (validity_date.item() + relativedelta.relativedelta(months=1, day=1)).strftime("%B")
    validity_date_str = validity_date.item().strftime("%d_%b_%y").upper()
    
    return df, validity_date_str, forecast_cycle


def create_output_folders(validity_date, forecast_cycle):
    """Create folders for output files.
    
    Args:
        validity_date: Validity date string
        forecast_cycle: Forecast cycle name
        
    Returns:
        tuple: (main_folder, sub_folder)
    """
    logger.info("Creating output folders")
    
    # Create folder names
    main_folder = f"BOM EO Forecast Snapshot on {validity_date} for {forecast_cycle} Forecast Cycle"
    sub_folder = f"BOM EO Forecast Parquet"
    
    # Create folders
    main_path = Path(main_folder)
    sub_path = main_path / sub_folder
    
    if not main_path.exists():
        main_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created folder: {main_path}")
        
        sub_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created folder: {sub_path}")
    
    return main_folder, sub_folder


def export_to_excel(df, validity_date, main_folder):
    """Export data to Excel files by market.
    
    Args:
        df: DataFrame with forecast data
        validity_date: Validity date string
        main_folder: Output folder path
    """
    logger.info("Exporting data to Excel files by market")
    
    # Get all markets
    markets = df["DP_GROUP_MKT"].unique()
    markets = [x for x in markets if not pd.isnull(x)]
    markets.sort()
    
    # Create folder if it doesn't exist
    main_path = Path(main_folder)
    main_path.mkdir(parents=True, exist_ok=True)
    
    # Export data for each market
    for market in markets:
        market_df = df[df["DP_GROUP_MKT"] == market]
        row_count = market_df.shape[0]
        
        output_file = main_path / f"BOM_EO_Forecast_{validity_date}_{market}.xlsx"
        market_df.to_excel(output_file, sheet_name="DATA", index=False)
        
        logger.info(f"Exported {row_count} rows for market {market} to {output_file}")


def export_to_parquet(df, validity_date, main_folder, sub_folder):
    """Export data to Parquet files.
    
    Args:
        df: DataFrame with forecast data
        validity_date: Validity date string
        main_folder: Output folder path
        sub_folder: Parquet subfolder path
    """
    logger.info("Exporting data to Parquet files")
    
    # Create path
    output_path = Path(main_folder) / sub_folder
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Define name function
    name_function = lambda x: f"BOM_EO_Forecast_{validity_date}-{x}.parquet"
    
    # Convert to dask DataFrame and export to parquet
    ddf = da.from_pandas(df, chunksize=5000000)
    ddf.to_parquet(
        str(output_path),
        name_function=name_function,
        engine="auto", 
        compression="snappy"
    )
    
    logger.info(f"Exported data to Parquet files in {output_path}")


def download_forecast(generate_excel=True, generate_parquet=True, open_drive=False):
    """Download forecast data and export to files.
    
    Args:
        generate_excel: Whether to generate Excel files
        generate_parquet: Whether to generate Parquet files
        open_drive: Whether to open Google Drive after completion
    """
    start_time = time.time()
    
    try:
        # Print welcome message
        print("""
------------------------------------------------------------------------------------
                Generate Regional Forecast Submarket Excel Files
------------------------------------------------------------------------------------
The following script will generate Excel files of the monthly Regional Forecast Data 
deaggregated at the Site level for distribution and reporting purposes.

The script will automatically create a folder and name it as per the corresponding
Forecast Cycle and create Excel files in the folder for each Submarket.
------------------------------------------------------------------------------------
        """)
        
        # Get forecast data
        df, validity_date, forecast_cycle = get_forecast_data()
        
        # Create output folders
        main_folder, sub_folder = create_output_folders(validity_date, forecast_cycle)
        
        # Export to Excel if requested
        if generate_excel:
            export_to_excel(df, validity_date, main_folder)
        else:
            logger.info("Skipping Excel export as requested")
        
        # Export to Parquet if requested
        if generate_parquet:
            export_to_parquet(df, validity_date, main_folder, sub_folder)
        else:
            logger.info("Skipping Parquet export as requested")
        
        # Calculate and display runtime
        end_time = time.time()
        runtime = end_time - start_time
        logger.info(f"Completed in {runtime:.3f} seconds")
        print(f"\nThe total time to run the script was {runtime:.3f} seconds.\n")
        
        # Open Google Drive if requested
        if open_drive:
            drive_url = config.get('app', 'drive_url')
            if drive_url:
                logger.info("Opening Google Drive in browser")
                print("For the final step, upload the BOM EO Forecast Snapshot folder to Google Drive.")
                webbrowser.open(drive_url)
            else:
                logger.warning("Google Drive URL not configured")
                print("For the final step, upload the BOM EO Forecast Snapshot folder to Google Drive.")
        
        # Success message
        print(f"\nSuccess! Files have been created in folder: {main_folder}")
        
    except Exception as e:
        logger.exception("Error in download_forecast")
        print(f"Error: {str(e)}")
        print("See log file for details.")
