import os
from typing import List

import duckdb
import polars as pl
from dotenv import load_dotenv

load_dotenv()


class DuckDB:
    def __init__(self):
        self.db_name = "assets"
        self.conn = duckdb.connect(f"{self.db_name}.duckdb")

    def create_tables(self) -> None:
        # Drop existing tables first
        self.conn.execute("DROP TABLE IF EXISTS exchange_rates CASCADE")
        self.conn.execute("DROP TABLE IF EXISTS asset_values CASCADE")
        self.conn.execute("DROP TABLE IF EXISTS assets CASCADE")
        self.conn.execute("DROP TABLE IF EXISTS currency_pairs CASCADE")
        self.conn.execute("DROP TABLE IF EXISTS currencies CASCADE")

        self.create_currencies_table()
        self.create_currency_pairs_table()
        self.create_assets_table()
        self.create_assets_value_table()
        self.create_exchange_rates_table()
        self.create_indexes()

    def drop_db(self) -> None:
        self.conn.close()
        db_path = f"{self.db_name}.duckdb"
        if os.path.exists(db_path):
            os.remove(db_path)

    def create_assets_table(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS assets (
                id INTEGER PRIMARY KEY,
                currency_id INTEGER NOT NULL,
                name VARCHAR NOT NULL,
                FOREIGN KEY (currency_id) REFERENCES currencies(id)
            )
        """)

        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS assets_id_seq START 1
        """)

    def create_currencies_table(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS currencies (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL
            )
        """)

        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS currencies_id_seq START 1
        """)

    def create_assets_value_table(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS asset_values (
                id INTEGER PRIMARY KEY,
                asset_id INTEGER NOT NULL,
                value DECIMAL(10,2) NOT NULL,
                date TIMESTAMP NOT NULL,
                FOREIGN KEY (asset_id) REFERENCES assets(id)
            )
        """)

        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS asset_values_id_seq START 1
        """)

    def create_exchange_rates_table(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS exchange_rates (
                currency_pair_id INTEGER NOT NULL,
                value DECIMAL(10,5) NOT NULL,
                date TIMESTAMP NOT NULL,
                PRIMARY KEY (currency_pair_id, date),
                FOREIGN KEY (currency_pair_id) REFERENCES currency_pairs(id)
            )
        """)

    def create_currency_pairs_table(self) -> None:
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS currency_pairs(
                id INTEGER PRIMARY KEY,
                base_currency_id INTEGER NOT NULL,
                quote_currency_id INTEGER NOT NULL,
                FOREIGN KEY (base_currency_id) REFERENCES currencies(id),
                FOREIGN KEY (quote_currency_id) REFERENCES currencies(id)
            )
            """)

    def create_indexes(self) -> None:
        # Create indexes for better query performance
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_assets_currency_id
            ON assets(currency_id)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_assets_values_date
            ON asset_values(date)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_exchange_rates_date
            ON exchange_rates(date)
        """)

        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_asset_values_asset_date
            ON asset_values(asset_id, date)
        """)

    def insert_data(self, dfs: List[pl.DataFrame]) -> None:
        (
            df_currencies,
            df_currency_pairs,
            df_assets,
            df_asset_values,
            df_exchange_rates,
        ) = dfs

        self.conn.execute(
            "INSERT INTO currencies SELECT ROW_NUMBER() OVER () as id, * FROM df_currencies"
        )
        self.conn.execute(
            "INSERT INTO currency_pairs SELECT ROW_NUMBER() OVER () as id, * FROM df_currency_pairs"
        )
        self.conn.execute("INSERT INTO assets SELECT * FROM df_assets")
        self.conn.execute(
            "INSERT INTO asset_values SELECT ROW_NUMBER() OVER () as id, * FROM df_asset_values"
        )
        self.conn.execute("INSERT INTO exchange_rates SELECT * FROM df_exchange_rates")
