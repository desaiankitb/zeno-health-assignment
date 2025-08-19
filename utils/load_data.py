import pandas as pd
from sqlalchemy import create_engine, Engine
import os
from dotenv import load_dotenv
from typing import List, Optional, Tuple
import logging
from sqlalchemy.exc import SQLAlchemyError
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

class DataLoader:
    """
    Handles loading CSV files into a PostgreSQL database using SQLAlchemy and pandas.

    This class manages environment variable configuration, database connection,
    and the process of reading CSV files and loading them into database tables.
    """
    def __init__(self, csv_directory: str, files_to_load: List[str], if_exists: str = 'replace', max_workers: int = 4, max_retries: int = 3, retry_delay: float = 2.0) -> None:
        """
        Initializes the DataLoader with the CSV directory and list of files.

        Args:
            csv_directory (str): Path to the directory containing CSV files.
            files_to_load (List[str]): List of CSV file names to load.
            if_exists (str): Behavior when the table exists ('replace', 'fail', 'append').
            max_workers (int): Number of threads for parallel loading.
            max_retries (int): Number of times to retry a failed file load.
            retry_delay (float): Delay (in seconds) between retries.
        """
        load_dotenv()
        self.db_host: Optional[str] = os.getenv("DB_HOST")
        self.db_user: Optional[str] = os.getenv("DB_USER")
        self.db_password: Optional[str] = os.getenv("DB_PASSWORD")
        self.db_name: Optional[str] = os.getenv("DB_NAME")
        self.csv_directory: str = csv_directory
        self.files_to_load: List[str] = files_to_load
        self.if_exists: str = if_exists
        self.max_workers: int = max_workers
        self.max_retries: int = max_retries
        self.retry_delay: float = retry_delay
        self.logger = logging.getLogger("DataLoader")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s')
        handler.setFormatter(formatter)
        if not self.logger.hasHandlers():
            self.logger.addHandler(handler)
        self.successful_files: List[str] = []
        self.failed_files: List[Tuple[str, str]] = []  # (filename, error)

    def _create_db_engine(self) -> Engine:
        """
        Creates a SQLAlchemy engine for connecting to the PostgreSQL database.

        Returns:
            Engine: SQLAlchemy Engine instance.

        Raises:
            ValueError: If any required database environment variable is missing.
        """
        if not all([self.db_host, self.db_user, self.db_password, self.db_name]):
            self.logger.error("Database credentials are not fully set in environment variables.")
            raise ValueError("Database credentials are not fully set in environment variables.")
        db_string: str = f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}/{self.db_name}"
        return create_engine(db_string)

    def validate_csv(self, df: pd.DataFrame, file_path: str) -> bool:
        """
        Perform basic validation on the DataFrame loaded from a CSV file.

        Args:
            df (pd.DataFrame): The DataFrame to validate.
            file_path (str): Path to the CSV file (for logging).

        Returns:
            bool: True if validation passes, False otherwise.
        """
        if df.empty:
            self.logger.error(f"CSV file '{file_path}' is empty.")
            return False
        if df.isnull().all(axis=1).any():
            self.logger.warning(f"CSV file '{file_path}' contains completely empty rows.")
        if df.columns.duplicated().any():
            self.logger.error(f"CSV file '{file_path}' contains duplicate columns.")
            return False
        # Example: check for at least one column
        if len(df.columns) == 0:
            self.logger.error(f"CSV file '{file_path}' has no columns.")
            return False
        # Example: check for consistent row lengths (pandas ensures this)
        return True

    def load_csv_to_postgres(self, file_path: str, table_name: str) -> Tuple[str, bool, str]:
        """
        Loads a single CSV file into a PostgreSQL table using pandas and SQLAlchemy.
        Each call creates its own engine/connection for thread safety.
        Retries on failure up to max_retries times.

        Args:
            file_path (str): Path to the CSV file.
            table_name (str): Name of the table to load data into.

        Returns:
            Tuple[str, bool, str]: (file_path, success, error_message)
        """
        last_error = ""
        for attempt in range(1, self.max_retries + 1):
            engine = None
            try:
                engine = self._create_db_engine()
                with engine.begin() as conn:
                    self.logger.info(f"Loading data from {file_path} into table '{table_name}'... (Attempt {attempt})")
                    df: pd.DataFrame = pd.read_csv(file_path, dtype=str)
                    if not self.validate_csv(df, file_path):
                        self.logger.error(f"Validation failed for '{file_path}'. Skipping this file.")
                        return (file_path, False, "Validation failed")
                    df.to_sql(table_name, conn, if_exists=self.if_exists, index=False, method=None)
                    self.logger.info(f"Successfully loaded {len(df)} records into '{table_name}'.")
                    return (file_path, True, "")
            except pd.errors.EmptyDataError:
                self.logger.error(f"CSV file '{file_path}' is empty or malformed.")
                last_error = "Empty or malformed CSV"
                break  # No point retrying
            except Exception as e:
                self.logger.warning(f"Attempt {attempt} failed for {file_path}: {e}")
                last_error = str(e)
                if attempt < self.max_retries:
                    self.logger.info(f"Retrying {file_path} in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
            finally:
                if engine is not None:
                    engine.dispose()
        self.logger.error(f"All {self.max_retries} attempts failed for {file_path}.")
        return (file_path, False, last_error)

    def run(self) -> None:
        """
        Orchestrates the process of loading all specified CSV files into the database in parallel.
        Each file is loaded in a separate thread with its own connection/transaction.
        Provides a summary report at the end.
        """
        futures = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for file_name in self.files_to_load:
                file_path: str = os.path.join(self.csv_directory, file_name)
                table_name: str = os.path.splitext(file_name)[0]
                futures.append(executor.submit(self.load_csv_to_postgres, file_path, table_name))
            for future in as_completed(futures):
                file_path, success, error = future.result()
                if success:
                    self.successful_files.append(file_path)
                else:
                    self.failed_files.append((file_path, error))
        self._summary_report()

    def _summary_report(self) -> None:
        """
        Prints a summary report of which files succeeded and which failed.
        """
        self.logger.info("\n===== Data Load Summary =====")
        self.logger.info(f"Successful files ({len(self.successful_files)}):")
        for f in self.successful_files:
            self.logger.info(f"  - {f}")
        self.logger.info(f"Failed files ({len(self.failed_files)}):")
        for f, reason in self.failed_files:
            self.logger.info(f"  - {f}: {reason}")
        self.logger.info("============================\n")

def main() -> None:
    """
    Entry point for running the DataLoader script.
    """
    csv_directory: str = "data/data-1"
    files_to_load: List[str] = [
        "olist_customers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "product_category_name_translation.csv"
    ]
    # You can change if_exists to 'fail' or 'append' as needed
    loader = DataLoader(csv_directory, files_to_load, if_exists='replace', max_workers=4, max_retries=3, retry_delay=2.0)
    loader.run()

if __name__ == "__main__":
    main()