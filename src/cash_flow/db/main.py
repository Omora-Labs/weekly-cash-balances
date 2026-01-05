from kairos_assets.data_gen.main import generate_data
from kairos_assets.db.setup import DuckDB


def setup_db() -> DuckDB:
    """
    Set up the database schema for assets.
    """
    db = DuckDB()
    print("Setup DuckDB Local instance")
    try:
        db.conn.execute("BEGIN TRANSACTION")
        print("Creating tables...")
        db.create_tables()
        print("Generating data...")
        data = generate_data()
        print("Inserting data...")
        db.insert_data(data)
        db.conn.execute("COMMIT")
        return db
    except Exception as e:
        db.conn.execute("ROLLBACK")
        db.drop_db()
        print(f"Setup failed: {e}")
        raise


if __name__ == "__main__":
    setup_db()
