"""Data transformation operations."""

import logging
from datetime import datetime

import pandas as pd
from dateutil import relativedelta

logger = logging.getLogger(__name__)


class DataTransformer:
    """Class for transforming dataframes."""
    
    @staticmethod
    def clean_encoding(df):
        """Clean string encoding issues in a DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame: Cleaned DataFrame
        """
        logger.info("Cleaning string encoding in DataFrame")
        
        # Make a copy to avoid modifying the original
        cleaned_df = df.copy()
        
        # Apply encoding fix to string columns
        cleaned_df = cleaned_df.applymap(
            lambda x: x.encode("unicode_escape").decode("utf-8") if isinstance(x, str) else x
        )
        
        return cleaned_df
    
    @staticmethod
    def standardize_column_names(df, column_mapping=None):
        """Standardize column names in a DataFrame.
        
        Args:
            df: Input DataFrame
            column_mapping: Dictionary of {old_name: new_name}
            
        Returns:
            DataFrame: DataFrame with standardized column names
        """
        logger.info("Standardizing column names in DataFrame")
        
        # Make a copy to avoid modifying the original
        std_df = df.copy()
        
        # Apply column mapping if provided
        if column_mapping:
            std_df = std_df.rename(columns=column_mapping)
            
        # Standardize remaining column names
        std_df.columns = [
            col.lower().replace(" ", "_") for col in std_df.columns
        ]
        
        return std_df
    
    @staticmethod
    def extract_date_features(df, date_column):
        """Extract date features from a date column.
        
        Args:
            df: Input DataFrame
            date_column: Name of the date column
            
        Returns:
            DataFrame: DataFrame with additional date features
        """
        logger.info(f"Extracting date features from column: {date_column}")
        
        # Make a copy to avoid modifying the original
        result_df = df.copy()
        
        # Convert column to datetime if not already
        if not pd.api.types.is_datetime64_any_dtype(result_df[date_column]):
            result_df[date_column] = pd.to_datetime(result_df[date_column])
        
        # Extract date features
        result_df[f"{date_column}_year"] = result_df[date_column].dt.year
        result_df[f"{date_column}_month"] = result_df[date_column].dt.month
        result_df[f"{date_column}_day"] = result_df[date_column].dt.day
        result_df[f"{date_column}_quarter"] = result_df[date_column].dt.quarter
        
        return result_df
    
    @staticmethod
    def get_forecast_metadata(df, validity_date_column="VALIDITY_DATE"):
        """Get forecast metadata from DataFrame.
        
        Args:
            df: Forecast DataFrame
            validity_date_column: Name of validity date column
            
        Returns:
            tuple: (validity_date_str, forecast_cycle)
        """
        logger.info("Extracting forecast metadata")
        
        # Get unique validity date
        validity_date = pd.to_datetime(df[validity_date_column]).dt.date.unique()
        
        if len(validity_date) == 0:
            raise ValueError(f"No validity dates found in column: {validity_date_column}")
        
        # Convert to date object
        validity_date_obj = validity_date.item()
        
        # Create formatted string
        validity_date_str = validity_date_obj.strftime("%d_%b_%y").upper()
        
        # Calculate forecast cycle (next month)
        forecast_date = validity_date_obj + relativedelta.relativedelta(months=1, day=1)
        forecast_cycle = forecast_date.strftime("%B")
        
        return validity_date_str, forecast_cycle
    
    @staticmethod
    def filter_dataframe(df, filter_dict):
        """Filter DataFrame based on conditions.
        
        Args:
            df: Input DataFrame
            filter_dict: Dictionary of {column: value} pairs to filter on
            
        Returns:
            DataFrame: Filtered DataFrame
        """
        logger.info(f"Filtering DataFrame with conditions: {filter_dict}")
        
        # Make a copy to avoid modifying the original
        filtered_df = df.copy()
        
        # Apply each filter condition
        for column, value in filter_dict.items():
            if column in filtered_df.columns:
                filtered_df = filtered_df[filtered_df[column] == value]
            else:
                logger.warning(f"Column not found for filtering: {column}")
        
        logger.info(f"Filtering resulted in {filtered_df.shape[0]} rows")
        return filtered_df
    
    @staticmethod
    def calculate_derived_metrics(df, source_col1, source_col2, result_col, operation="subtract"):
        """Calculate derived metrics between columns.
        
        Args:
            df: Input DataFrame
            source_col1: First source column
            source_col2: Second source column
            result_col: Result column name
            operation: Operation to perform (subtract, add, multiply, divide)
            
        Returns:
            DataFrame: DataFrame with calculated metrics
        """
        logger.info(f"Calculating derived metric: {result_col} from {source_col1} and {source_col2}")
        
        # Make a copy to avoid modifying the original
        result_df = df.copy()
        
        # Check if source columns exist
        if source_col1 not in result_df.columns or source_col2 not in result_df.columns:
            missing = []
            if source_col1 not in result_df.columns:
                missing.append(source_col1)
            if source_col2 not in result_df.columns:
                missing.append(source_col2)
            raise ValueError(f"Source columns not found: {', '.join(missing)}")
        
        # Perform the calculation based on the operation
        if operation == "subtract":
            result_df[result_col] = result_df[source_col1] - result_df[source_col2]
        elif operation == "add":
            result_df[result_col] = result_df[source_col1] + result_df[source_col2]
        elif operation == "multiply":
            result_df[result_col] = result_df[source_col1] * result_df[source_col2]
        elif operation == "divide":
            # Avoid division by zero
            result_df[result_col] = result_df[source_col1] / result_df[source_col2].replace(0, float('nan'))
        else:
            raise ValueError(f"Unsupported operation: {operation}")
        
        return result_df
