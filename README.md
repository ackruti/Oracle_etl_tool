
# Oracle ETL Tool

A command-line tool for ETL operations with Oracle databases.

## Features

- Download BOM EO Forecast data and export to Excel/Parquet
- Upload data from files to Oracle database tables
- Secure credential management
- Configurable via YAML configuration
- Comprehensive logging

## Installation

### Prerequisites

- Python 3.8 or higher
- Oracle Instant Client (18.5 or 19.3 recommended)
  - Default paths are: 
    - `C:\DevTools\Oracle Instant Client 18.5\instantclient_18_5`
    - `C:\Oracle\instantclient_19_3`
  - If installed elsewhere, update the paths in `config/config.yaml`

### Option 1: Clone from GitHub

```bash
# Clone the repository
git clone https://github.com/yourusername/oracle-etl-tool.git
cd oracle-etl-tool

# Install the package in development mode
pip install -e .
```

### Option 2: Local Installation

If you received the code as a zip file or through other means:

```bash
# Navigate to the project directory
cd path/to/oracle_etl_tool

# Install the package in development mode
pip install -e .
```

## Usage

### Command-line Interface

The tool provides a simple command-line interface:

```
oracle-etl [--reset-credentials] [--debug] <command> [options]
```

Available commands:

- `download-forecast`: Download BOM EO Forecast data and export to Excel/Parquet
- `upload-data`: Upload data from a file to the Oracle database

### Download Forecast Data

```
oracle-etl download-forecast [--no-excel] [--no-parquet] [--open-drive]
```

Options:
- `--no-excel`: Skip Excel file generation
- `--no-parquet`: Skip Parquet file generation
- `--open-drive`: Open Google Drive in browser after completion

### Upload Data to Database

```
oracle-etl upload-data [--file FILENAME] [--table TABLENAME]
```

Options:
- `--file`: File to upload (if not specified, will prompt for selection)
- `--table`: Target table name (default: t_ibp_cons_rdc)

### Reset Credentials

```
oracle-etl --reset-credentials
```

## Configuration

Configuration is stored in `config/config.yaml`. The main settings include:

- Database connection details
- Oracle client paths
- SQL queries
- Application settings

## For Non-Technical Users

### First-time Setup

1. Ensure Oracle Instant Client is installed on your computer
2. Open Command Prompt (search for "cmd" in Windows)
3. Navigate to the tool directory (use `cd` command)
4. Run: `pip install -e .`

### Downloading Forecast Data

1. Open Command Prompt
2. Navigate to the tool directory
3. Run: `oracle-etl download-forecast`
4. Enter your Oracle database credentials when prompted
5. The tool will create a folder with the forecast data files

### Uploading Data

1. Open Command Prompt
2. Navigate to the tool directory
3. Run: `oracle-etl upload-data`
4. Enter your Oracle database credentials when prompted
5. Select the file to upload from the displayed list
6. The tool will upload the data to the database

### Troubleshooting

If you encounter any issues:
- Check the logs in the `logs` directory
- Run the command with `--debug` for more detailed logging
- If credentials are incorrect, run with `--reset-credentials`
