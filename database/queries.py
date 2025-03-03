"""SQL queries for database operations."""

import logging
from ..config.config import config

logger = logging.getLogger(__name__)


class QueryBuilder:
    """Helper class to build and manage SQL queries."""
    
    @staticmethod
    def get_forecast_query():
        """Get the BOM forecast query from config.
        
        Returns:
            str: SQL query for BOM forecast data
        """
        query = config.get_query('bomfc_dpv_detail_hist')
        if not query:
            logger.warning("Forecast query not found in config, using default query")
            query = """
            SELECT *
            FROM network_rw.bomfc_dpv_detail_hist
            WHERE TRUNC(validity_date) = (SELECT TRUNC(MAX(validity_date)) FROM network_rw.bomfc_dpv_detail_hist)
            """
        return query
    
    @staticmethod
    def get_max_validity_date(table_name, schema=None):
        """Build a query to get the maximum validity date from a table.
        
        Args:
            table_name: Name of the table
            schema: Schema name (optional)
            
        Returns:
            str: SQL query to get max validity date
        """
        if schema:
            full_table_name = f"{schema}.{table_name}"
        else:
            full_table_name = table_name
            
        return f"SELECT MAX(validity_date) as max_date FROM {full_table_name}"
    
    @staticmethod
    def build_insert_query(table_name, columns, schema=None):
        """Build an SQL INSERT query.
        
        Args:
            table_name: Target table name
            columns: List of column names
            schema: Schema name (optional)
            
        Returns:
            str: SQL INSERT query
        """
        if schema:
            full_table_name = f"{schema}.{table_name}"
        else:
            full_table_name = table_name
            
        placeholders = ", ".join([":" + col for col in columns])
        column_list = ", ".join(columns)
        
        return f"INSERT INTO {full_table_name} ({column_list}) VALUES ({placeholders})"
    
    @staticmethod
    def build_select_query(table_name, columns=None, where=None, schema=None):
        """Build an SQL SELECT query.
        
        Args:
            table_name: Target table name
            columns: List of column names (optional, defaults to *)
            where: WHERE clause (optional)
            schema: Schema name (optional)
            
        Returns:
            str: SQL SELECT query
        """
        if schema:
            full_table_name = f"{schema}.{table_name}"
        else:
            full_table_name = table_name
            
        if columns:
            column_list = ", ".join(columns)
        else:
            column_list = "*"
            
        query = f"SELECT {column_list} FROM {full_table_name}"
        
        if where:
            query += f" WHERE {where}"
            
        return query
