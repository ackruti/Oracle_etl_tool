"""Configuration module for Oracle ETL Tool."""

import os
import yaml
from pathlib import Path


class Config:
    """Configuration class to load and access settings."""
    
    _instance = None
    
    def __new__(cls):
        """Singleton pattern implementation."""
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._loaded = False
        return cls._instance
    
    def __init__(self):
        """Initialize configuration if not already loaded."""
        if not self._loaded:
            self._config_dir = Path(__file__).parent
            self._config_file = self._config_dir / "config.yaml"
            self._config = self._load_config()
            self._loaded = True
    
    def _load_config(self):
        """Load configuration from YAML file."""
        if not self._config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {self._config_file}")
        
        with open(self._config_file, 'r') as f:
            return yaml.safe_load(f)
    
    def get(self, *keys, default=None):
        """Get configuration value using dot notation."""
        config = self._config
        for key in keys:
            if key in config:
                config = config[key]
            else:
                return default
        return config
    
    def get_oracle_client_path(self):
        """Get the first available Oracle client path."""
        paths = self.get('oracle_client', 'paths', default=[])
        for path in paths:
            if os.path.exists(path):
                return path
        return None
    
    def get_database_url(self, schema, username, password, host_key='sp1'):
        """Generate database connection URL."""
        host = self.get('database', 'hosts', host_key, 'host')
        port = self.get('database', 'hosts', host_key, 'port')
        service = self.get('database', 'hosts', host_key, 'service')
        
        return f"{host}:{port}/{service}"
    
    def get_query(self, query_name):
        """Get SQL query by name."""
        return self.get('queries', query_name)


# Create a singleton instance
config = Config()
