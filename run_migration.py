import os
import sys
from alembic import command
from alembic.config import Config

def run_migrations():
    # Create Alembic configuration object
    alembic_cfg = Config("alembic.ini")
    
    # Ensure the script directory is in sys.path so env.py can import app
    sys.path.insert(0, os.getcwd())

    print("Generating migration...")
    try:
        command.revision(alembic_cfg, autogenerate=True, message="sync_schema")
        print("Migration generated.")
    except Exception as e:
        print(f"Failed to generate migration: {e}")
        # Continue to upgrade in case migration was already generated but not applied
    
    print("Applying migration...")
    try:
        command.upgrade(alembic_cfg, "head")
        print("Migration applied successfully.")
    except Exception as e:
        print(f"Failed to apply migration: {e}")

if __name__ == "__main__":
    run_migrations()
