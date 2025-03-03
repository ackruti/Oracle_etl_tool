"""Helper functions for the ETL tool."""

import logging
import os
import time
import webbrowser
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def measure_execution_time(func):
    """Decorator to measure function execution time.
    
    Args:
        func: Function to measure
        
    Returns:
        Wrapped function
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger.info(f"Starting {func.__name__}")
        
        result = func(*args, **kwargs)
        
        end_time = time.time()
        execution_time = end_time - start_time
        logger.info(f"Completed {func.__name__} in {execution_time:.3f} seconds")
        
        return result
    
    return wrapper


def get_timestamp_str(format_str="%Y%m%d_%H%M%S"):
    """Get a formatted timestamp string.
    
    Args:
        format_str: Timestamp format
        
    Returns:
        str: Formatted timestamp
    """
    return datetime.now().strftime(format_str)


def create_directory_if_not_exists(directory_path):
    """Create directory if it doesn't exist.
    
    Args:
        directory_path: Path to directory
        
    Returns:
        Path: Path to directory
    """
    path = Path(directory_path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def open_file_explorer(path):
    """Open file explorer to the specified path.
    
    Args:
        path: Path to open
    """
    path = os.path.normpath(path)
    
    if os.path.isdir(path):
        try:
            if os.name == 'nt':  # Windows
                os.startfile(path)
            elif os.name == 'posix':  # macOS and Linux
                import subprocess
                if 'darwin' in os.uname().sysname.lower():  # macOS
                    subprocess.call(['open', path])
                else:  # Linux
                    subprocess.call(['xdg-open', path])
            logger.info(f"Opened file explorer to: {path}")
        except Exception as e:
            logger.warning(f"Failed to open file explorer: {e}")
    else:
        logger.warning(f"Path does not exist: {path}")


def open_browser(url):
    """Open web browser to the specified URL.
    
    Args:
        url: URL to open
        
    Returns:
        bool: Success status
    """
    if not url:
        logger.warning("No URL provided")
        return False
    
    try:
        webbrowser.open(url)
        logger.info(f"Opened browser to: {url}")
        return True
    except Exception as e:
        logger.warning(f"Failed to open browser: {e}")
        return False


def format_file_size(size_bytes):
    """Format file size in human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        str: Formatted size
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ("B", "KB", "MB", "GB", "TB", "PB")
    i = 0
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024
        i += 1
    
    return f"{size_bytes:.2f} {size_names[i]}"


def list_directory_contents(directory=".", include_files=True, include_dirs=True, recursive=False):
    """List contents of a directory.
    
    Args:
        directory: Directory path
        include_files: Whether to include files
        include_dirs: Whether to include directories
        recursive: Whether to include subdirectories recursively
        
    Returns:
        list: Directory contents
    """
    contents = []
    
    try:
        if recursive:
            for root, dirs, files in os.walk(directory):
                if include_dirs:
                    for dir_name in dirs:
                        dir_path = os.path.join(root, dir_name)
                        contents.append(dir_path)
                
                if include_files:
                    for file_name in files:
                        file_path = os.path.join(root, file_name)
                        contents.append(file_path)
        else:
            with os.scandir(directory) as entries:
                for entry in entries:
                    if (entry.is_file() and include_files) or (entry.is_dir() an
