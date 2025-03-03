from setuptools import setup, find_packages

setup(
    name="oracle_etl_tool",
    version="1.0.0",
    description="ETL tools for Oracle databases",
    author="Your Organization",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "cx_Oracle>=8.3.0",
        "dask>=2023.3.0",
        "numpy>=1.23.0",
        "pandas>=1.5.0",
        "PyYAML>=6.0",
        "sqlalchemy>=1.4.0",
    ],
    entry_points={
        "console_scripts": [
            "oracle-etl=oracle_etl_tool.__main__:main",
        ],
    },
)
