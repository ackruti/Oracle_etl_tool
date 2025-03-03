"""Command-line interface for Oracle ETL Tool."""

import argparse
import logging
import os
import sys
import webbrowser
from pathlib import Path

from .config.config import config
from .scripts.download_forecast import download_forecast
from .scripts.upload_data import upload_data
from .utils.logging_config import setup_logging


def setup_cli():
    """Set up command-line interface."""
    parser = argparse.ArgumentParser(
        description="Oracle ETL Tool for Oracle database operations",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Global options
    parser.add_argument(
        "--reset-credentials",
        action="store_true",
        help="Reset the stored database credentials"
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    
    # Subcommands
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Download forecast command
    download_parser = subparsers.add_parser(
        "download-forecast",
        help="Download BOM EO Forecast data and export to Excel/Parquet"
    )
    download_parser.add_argument(
        "--no-excel",
        action="store_true",
        help="Skip Excel file generation"
    )
    download_parser.add_argument(
        "--no-parquet",
        action="store_true",
        help="Skip Parquet file generation"
    )
    download_parser.add_argument(
        "--open-drive",
        action="store_true",
        help="Open Google Drive in browser after completion"
    )
    
    # Upload data command
    upload_parser = subparsers.add_parser(
        "upload-data",
        help="Upload data from a file to the Oracle database"
    )
    upload_parser.add_argument(
        "--file",
        type=str,
        help="File to upload (if not specified, will prompt for selection)"
    )
    upload_parser.add_argument(
        "--table",
        type=str,
        default="t_ibp_cons_rdc",
        help="Target table name (default: t_ibp_cons_rdc)"
    )
    
    return parser


def main():
    """Main entry point for the CLI application."""
    parser = setup_cli()
    args = parser.parse_args()
    
    # Set up logging
    log_level = logging.DEBUG if args.debug else logging.INFO
    logger = setup_logging(log_level=log_level)
    
    # Handle reset credentials
    if args.reset_credentials:
        credentials_file = config.get('app', 'credentials_file')
        if os.path.exists(credentials_file):
            os.remove(credentials_file)
            logger.info(f"Credentials reset - deleted {credentials_file}")
        else:
            logger.info("No credentials file found to reset")
    
    # Execute the appropriate command
    if args.command == "download-forecast":
        download_forecast(
            generate_excel=(not args.no_excel),
            generate_parquet=(not args.no_parquet),
            open_drive=args.open_drive
        )
    elif args.command == "upload-data":
        upload_data(
            filename=args.file,
            tablename=args.table
        )
    else:
        # No command specified, show help
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
