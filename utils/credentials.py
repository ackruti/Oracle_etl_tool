"""Credential management utilities."""

import json
import os
from pathlib import Path

from ..config.config import config


class CredentialManager:
    """Manage database credentials."""
    
    def __init__(self, credentials_file=None):
        """Initialize credential manager.
        
        Args:
            credentials_file: Path to credentials file. If None, uses default from config.
        """
        self.credentials_file = credentials_file or config.get('app', 'credentials_file')
        
    def credentials_exist(self):
        """Check if credentials file exists."""
        return os.path.exists(self.credentials_file)
    
    def save_credentials(self, username, password):
        """Save credentials to file.
        
        Args:
            username: Database username
            password: Database password
        """
        credentials = {
            "username": username.strip().lower(),
            "password": password.strip()
        }
        
        with open(self.credentials_file, 'w') as f:
            json.dump(credentials, f)
            
    def load_credentials(self):
        """Load credentials from file.
        
        Returns:
            tuple: (username, password)
        """
        if not self.credentials_exist():
            return None, None
            
        with open(self.credentials_file, 'r') as f:
            data = json.load(f)
            return data.get('username'), data.get('password')
            
    def prompt_for_credentials(self, prompt_text=None):
        """Prompt user for credentials interactively.
        
        Args:
            prompt_text: Text to display before prompting
            
        Returns:
            tuple: (username, password)
        """
        if prompt_text:
            print(prompt_text)
        else:
            print("""
Oracle SQL Credentials are required to run this tool. The credentials
are the same Username and Password required to access the Oracle Database.

NOTE: To reset credentials, delete the credentials.json file in the current folder.
""")
        
        username = input("Username: ").strip().lower()
        password = input("Password: ").strip()
        
        # Save the credentials
        self.save_credentials(username, password)
        
        return username, password
        
    def get_credentials(self, force_prompt=False):
        """Get credentials, prompting if necessary.
        
        Args:
            force_prompt: Force prompting even if credentials exist
            
        Returns:
            tuple: (username, password)
        """
        if force_prompt or not self.credentials_exist():
            return self.prompt_for_credentials()
        
        return self.load_credentials()

# Create instance for easy import
credential_manager = CredentialManager()
