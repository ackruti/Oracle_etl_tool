"""Data loading operations."""

import logging
import os
from datetime import datetime
from pathlib import Path

import dask.dataframe as da
import pandas as pd
from sqlalchemy import MetaData, Table, inspect

logger = logging.getLogger(__name__)


class DataLoader:
    """Class for loading data to various destinations."""
    
    @staticmethod
    def load_to_excel(df, output_path, sheet_name="DATA", index=False, **kwargs):
        """Load data to Excel file.
        
        Args:
            df: DataFrame to save
            output_path: Path to output Excel file
            sheet_name: Excel sheet name
            index: Whether to include index in output
            **kwargs: Additional arguments for to_excel
            
        Returns:
            str: Path to saved file
        """
        logger.info(f"Saving {df.shape[0]} rows to Excel file: {output_path}")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to Excel
        df.to_excel(output_path, sheet_name=sheet_name, index=index, **kwargs)
        
        logger.info(f"Successfully saved data to: {output_path}")
        return output_path
    
    @staticmethod
    def load_to_parquet(df, output_dir, name_function=None, engine="auto", compression="snappy", **kwargs):
        """Load data to Parquet files.
        
        Args:
            df: DataFrame to save
            output_dir: Output directory
            name_function: Function to generate filenames
            engine: Parquet engine
            compression: Compression method
            **kwargs: Additional arguments for to_parquet
            
        Returns:
            str: Path to saved directory
        """
        logger.info(f"Saving {df.shape[0]} rows to Parquet files in: {output_dir}")
        
        # Ensure directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Convert to dask DataFrame for partitioning
        ddf = da.from_pandas(df, chunksize=5000000)
        
        # Save to parquet
        ddf.to_parquet(
            output_dir,
            name_function=name_function,
            engine=engine,
            compression=compression,
            **kwargs
        )
        
        logger.info(f"Successfully saved data to Parquet files in: {output_dir}")
        return output_dir
    
    @staticmethod
    def load_to_csv(df, output_path, index=False, **kwargs):
        """Load data to CSV file.
        
        Args:
            df: DataFrame to save
            output_path: Path to output CSV file
            index: Whether to include index in output
            **kwargs: Additional arguments for to_csv
            
        Returns:
            str: Path to saved file
        """
        logger.info(f"Saving {df.shape[0]} rows to CSV file: {output_path}")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to CSV
        df.to_csv(output_path, index=index, **kwargs)
        
        logger.info(f"Successfully saved data to: {output_path}")
        return output_path
    
    @staticmethod
    def load_to_database(df, engine, table_name, schema=None, if_exists="append", index=False, **kwargs):
        """Load data to database table.
        
        Args:
            df: DataFrame to save
            engine: SQLAlchemy engine
            table_name: Target table name
            schema: Database schema
            if_exists: How to behave if table exists
            index: Whether to include index
            **kwargs: Additional arguments for to_sql
            
        Returns:
            int: Number of rows loaded
        """
        logger.info(f"Loading {df.shape[0]} rows to database table: {table_name}")
        
        try:
            # Check if table exists
            inspector = inspect(engine)
            if not inspector.has_table(table_name, schema=schema):
                logger.warning(f"Table does not exist: {table_name}")
                if if_exists == "append" or if_exists == "replace":
                    logger.info(f"Creating table: {table_name}")
                else:
                    raise ValueError(f"Table does not exist and if_exists='{if_exists}'")
            
            # Upload to database
            df.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists=if_exists,
                index=index,
                **kwargs
            )
            
            logger.info(f"Successfully loaded {df.shape[0]} rows to table: {table_name}")
            return df.shape[0]
            
        except Exception as e:
            logger.exception(f"Error loading data to table: {table_name}")
            raise ValueError(f"Failed to load data to database: {str(e)}")
    
    @staticmethod
    def bulk_insert_to_database(df, engine, table_name, schema=None):
        """Perform bulk insert to database table.
        
        Args:
            df: DataFrame to insert
            engine: SQLAlchemy engine
            table_name: Target table name
            schema: Database schema
            
        Returns:
            int: Number of rows inserted
        """
        logger.info(f"Bulk inserting {df.shape[0]} rows to database table: {table_name}")
        
        try:
            # Get table metadata
            metadata = MetaData(schema=schema)
            metadata.reflect(bind=engine, only=[table_name])
            
            if table_name not in metadata.tables:
                raise ValueError(f"Table {table_name} does not exist in the database")
                
            table = metadata.tables[table_name if schema is None else f"{schema}.{table_name}"]
            
            # Convert DataFrame to records
            records = df.to_dict(orient='records')
            
            # Insert data
            with engine.begin() as connection:
                result = connection.execute(table.insert(), records)
                
            logger.info(f"Successfully inserted {len(records)} records into {table_name}")
            return len(records)
            
        except Exception as e:
            logger.exception(f"Error bulk inserting to table: {table_name}")
            raise ValueError(f"Failed to bulk insert to database: {str(e)}")
    
    @staticmethod
    def export_by_group(df, group_column, output_dir, file_prefix, file_suffix="xlsx", sheet_name="DATA", **kwargs):
        """Export DataFrame to separate files by group.
        
        Args:
            df: DataFrame to export
            group_column: Column to group by
            output_dir: Output directory
            file_prefix: Prefix for output files
            file_suffix: File suffix/format (xlsx, csv, etc.)
            sheet_name: Sheet name for Excel files
            **kwargs: Additional arguments for export functions
            
        Returns:
            list: List of saved file paths
        """
        logger.info(f"Exporting data by group column: {group_column}")
        
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Get unique group values
        groups = df[group_column].unique()
        groups = [x for x in groups if not pd.isnull(x)]
        groups.sort()
        
        saved_files = []
        
        # Export data for each group
        for group in groups:
            # Filter data for this group
            group_df = df[df[group_column] == group]
            row_count = group_df.shape[0]
            
            # Create output filename
            filename = f"{file_prefix}_{group}.{file_suffix}"
            output_path = os.path.join(output_dir, filename)
            
            # Export based on file type
            if file_suffix.lower() == "xlsx":
                group_df.to_excel(output_path, sheet_name=sheet_name, index=False, **kwargs)
            elif file_suffix.lower() == "csv":
                group_df.to_csv(output_path, index=False, **kwargs)
            elif file_suffix.lower() == "parquet":
                group_df.to_parquet(output_path, index=False, **kwargs)
            else:
                raise ValueError(f"Unsupported file type: {file_suffix}")
            
            logger.info(f"Exported {row_count} rows for {group_column}={group} to {output_path}")
            saved_files.append(output_path)
        
        return saved_files
