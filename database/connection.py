"""Database connection management."""

import logging
import os

import cx_Oracle
import pandas as pd
import sqlalchemy as sa

from ..config.config import config
from ..utils.credentials import credential_manager

logger = logging.getLogger(__name__)


class DatabaseConnectionError(Exception):
    """Exception raised for database connection errors."""
    pass


class OracleConnection:
    """Oracle database connection manager."""
    
    def __init__(self, host_key='sp1'):
        """Initialize connection manager.
        
        Args:
            host_key: Key for host configuration in config file
        """
        self.host_key = host_key
        self.connection = None
        self.engine = None
        
        # Set up Oracle client
        self._setup_oracle_client()
        
    def _setup_oracle_client(self):
        """Set up Oracle client libraries."""
        lib_dir = config.get_oracle_client_path()
        if not lib_dir:
            raise DatabaseConnectionError(
                "Oracle Driver directory is missing. Please install Oracle Instant Client."
            )
        
        try:
            # Initialize Oracle client
            cx_Oracle.init_oracle_client(lib_dir=lib_dir)
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to initialize Oracle client: {e}")
            
    def connect(self, username=None, password=None):
        """Connect to the Oracle database.
        
        Args:
            username: Database username (will prompt if None)
            password: Database password (will prompt if None)
            
        Returns:
            connection: Oracle connection object
        """
        # Get credentials if not provided
        if username is None or password is None:
            username, password = credential_manager.get_credentials()
            
        if not username or not password:
            raise DatabaseConnectionError("No credentials provided")
            
        try:
            # Get connection details from config
            host = config.get('database', 'hosts', self.host_key, 'host')
            port = config.get('database', 'hosts', self.host_key, 'port')
            service = config.get('database', 'hosts', self.host_key, 'service')
            
            # Create DSN and connection
            dsn_tns = cx_Oracle.makedsn(host, port, service_name=service)
            self.connection = cx_Oracle.connect(user=username, password=password, dsn=dsn_tns)
            logger.info(f"Connected to Oracle database {host}:{port}/{service}")
            
            return self.connection
            
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to connect to database: {e}")
            
    def create_sqlalchemy_engine(self, username=None, password=None):
        """Create SQLAlchemy engine for the Oracle database.
        
        Args:
            username: Database username (will prompt if None)
            password: Database password (will prompt if None)
            
        Returns:
            engine: SQLAlchemy engine
        """
        # Get credentials if not provided
        if username is None or password is None:
            username, password = credential_manager.get_credentials()
            
        if not username or not password:
            raise DatabaseConnectionError("No credentials provided")
            
        try:
            # Get connection details from config
            dialect = config.get('database', 'dialect')
            driver = config.get('database', 'driver')
            host = config.get('database', 'hosts', self.host_key, 'host')
            port = config.get('database', 'hosts', self.host_key, 'port')
            service = config.get('database', 'hosts', self.host_key, 'service')
            
            # Create engine
            engine_path = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/?service_name={service}"
            self.engine = sa.create_engine(engine_path)
            logger.info(f"Created SQLAlchemy engine for {host}:{port}/{service}")
            
            return self.engine
            
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create SQLAlchemy engine: {e}")
            
    def query_to_dataframe(self, sql, params=None):
        """Execute a SQL query and return the results as a DataFrame.
        
        Args:
            sql: SQL query to execute
            params: Parameters for query
            
        Returns:
            df: Pandas DataFrame with query results
        """
        if self.connection is None:
            self.connect()
            
        try:
            logger.info("Executing SQL query and loading results to DataFrame")
            df = pd.read_sql(sql, self.connection, params=params)
            logger.info(f"Query returned {df.shape[0]} rows and {df.shape[1]} columns")
            return df
            
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to execute query: {e}")
            
    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed")
            
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
