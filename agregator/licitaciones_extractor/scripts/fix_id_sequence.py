#!/usr/bin/env python3
"""
Script to fix the auto-increment ID sequence in the updates table.
This script resets the sequence to start from 1.
"""

import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.connection import DatabaseConnection
from src.utils.logger import get_logger, setup_logging

def fix_id_sequence():
    """Fix the auto-increment ID sequence in the updates table."""
    setup_logging()
    logger = get_logger("fix_id_sequence")

    logger.info("Starting ID sequence fix for updates table...")

    try:
        # Initialize database connection
        db_conn = DatabaseConnection()

        with db_conn.get_session() as session:
            # Check current state
            logger.info("Checking current table state...")

            # Get current max ID
            result = session.execute("SELECT MAX(id) as max_id FROM updates").fetchone()
            current_max_id = result[0] if result and result[0] else 0
            logger.info(f"Current max ID in table: {current_max_id}")

            # Get current sequence value
            try:
                seq_result = session.execute("SELECT last_value FROM updates_id_seq").fetchone()
                current_seq_value = seq_result[0] if seq_result else 0
                logger.info(f"Current sequence value: {current_seq_value}")
            except Exception as e:
                logger.warning(f"Could not get sequence value: {e}")
                current_seq_value = current_max_id

            # Get total number of records
            count_result = session.execute("SELECT COUNT(*) as total FROM updates").fetchone()
            total_records = count_result[0] if count_result else 0
            logger.info(f"Total records in table: {total_records}")

            # Show current ID distribution
            if total_records > 0:
                id_sample = session.execute("SELECT id FROM updates ORDER BY id LIMIT 10").fetchall()
                logger.info(f"First 10 IDs: {[row[0] for row in id_sample]}")

            # Ask for confirmation
            print(f"\nCurrent situation:")
            print(f"  - Total records: {total_records}")
            print(f"  - Current max ID: {current_max_id}")
            print(f"  - Current sequence: {current_seq_value}")

            if total_records == 0:
                logger.info("Table is empty, resetting sequence to 1")
                reset_to_one = True
            else:
                response = input(f"\nDo you want to reset all IDs to start from 1? (y/N): ")
                reset_to_one = response.lower().startswith('y')

            if reset_to_one:
                logger.info("Resetting ID sequence to start from 1...")

                if total_records > 0:
                    # Create a temporary column for new IDs
                    logger.info("Creating temporary ID column...")
                    session.execute("ALTER TABLE updates ADD COLUMN temp_id SERIAL")
                    session.commit()

                    # Drop the old primary key constraint
                    logger.info("Dropping old primary key...")
                    session.execute("ALTER TABLE updates DROP CONSTRAINT updates_pkey")
                    session.commit()

                    # Drop the old id column
                    logger.info("Dropping old ID column...")
                    session.execute("ALTER TABLE updates DROP COLUMN id")
                    session.commit()

                    # Rename temp_id to id
                    logger.info("Renaming new ID column...")
                    session.execute("ALTER TABLE updates RENAME COLUMN temp_id TO id")
                    session.commit()

                    # Add primary key constraint
                    logger.info("Adding new primary key constraint...")
                    session.execute("ALTER TABLE updates ADD PRIMARY KEY (id)")
                    session.commit()

                    logger.info("✅ Successfully reset ID sequence!")
                else:
                    # Table is empty, just reset the sequence
                    logger.info("Resetting sequence for empty table...")
                    session.execute("ALTER SEQUENCE updates_id_seq RESTART WITH 1")
                    session.commit()
                    logger.info("✅ Sequence reset to 1!")

                # Verify the fix
                logger.info("Verifying the fix...")
                verify_result = session.execute("SELECT nextval('updates_id_seq')").fetchone()
                next_id = verify_result[0] if verify_result else None

                if total_records == 0:
                    expected_next = 1
                else:
                    # Get new max ID
                    max_result = session.execute("SELECT MAX(id) FROM updates").fetchone()
                    expected_next = (max_result[0] + 1) if max_result and max_result[0] else 1

                logger.info(f"Next ID will be: {next_id}")

                if total_records > 0:
                    # Show new ID distribution
                    new_sample = session.execute("SELECT id FROM updates ORDER BY id LIMIT 10").fetchall()
                    logger.info(f"New first 10 IDs: {[row[0] for row in new_sample]}")

                # Reset sequence to correct value after our test
                session.execute(f"SELECT setval('updates_id_seq', {expected_next-1})")
                session.commit()

                print(f"\n🎉 ID sequence fix completed!")
                print(f"   - Next new record will have ID: {expected_next}")
                print(f"   - IDs now start from: 1")

            else:
                # Just fix the sequence to continue from max+1
                logger.info("Fixing sequence to continue from current max...")
                next_id = current_max_id + 1
                session.execute(f"SELECT setval('updates_id_seq', {current_max_id})")
                session.commit()
                logger.info(f"✅ Sequence fixed! Next ID will be: {next_id}")

    except Exception as e:
        logger.error(f"Error fixing ID sequence: {e}")
        raise

def check_id_sequence():
    """Check the current ID sequence status."""
    setup_logging()
    logger = get_logger("check_id_sequence")

    try:
        db_conn = DatabaseConnection()

        with db_conn.get_session() as session:
            # Table info
            count_result = session.execute("SELECT COUNT(*) FROM updates").fetchone()
            total_records = count_result[0] if count_result else 0

            if total_records == 0:
                print("📊 Table Status: EMPTY")
                return

            # ID analysis
            min_result = session.execute("SELECT MIN(id) FROM updates").fetchone()
            max_result = session.execute("SELECT MAX(id) FROM updates").fetchone()

            min_id = min_result[0] if min_result else 0
            max_id = max_result[0] if max_result else 0

            # Sequence info
            try:
                seq_result = session.execute("SELECT last_value FROM updates_id_seq").fetchone()
                seq_value = seq_result[0] if seq_result else 0
            except:
                seq_value = "N/A"

            # Sample IDs
            sample_result = session.execute("SELECT id FROM updates ORDER BY id LIMIT 5").fetchall()
            sample_ids = [row[0] for row in sample_result] if sample_result else []

            print("📊 Table Status:")
            print(f"   Total records: {total_records}")
            print(f"   ID range: {min_id} - {max_id}")
            print(f"   Current sequence: {seq_value}")
            print(f"   First 5 IDs: {sample_ids}")

            # Check if IDs are problematic
            if min_id != 1:
                print(f"⚠️  ISSUE: IDs don't start from 1 (start from {min_id})")
            else:
                print("✅ IDs start from 1 correctly")

            if total_records != (max_id - min_id + 1):
                print(f"⚠️  ISSUE: Missing IDs in sequence (gaps detected)")
            else:
                print("✅ No gaps in ID sequence")

    except Exception as e:
        logger.error(f"Error checking ID sequence: {e}")

def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Fix updates table ID sequence")
    parser.add_argument("--check", action="store_true", help="Only check current status")
    parser.add_argument("--fix", action="store_true", help="Fix the ID sequence")

    args = parser.parse_args()

    if args.check:
        check_id_sequence()
    elif args.fix:
        fix_id_sequence()
    else:
        print("Usage:")
        print("  python scripts/fix_id_sequence.py --check   # Check current status")
        print("  python scripts/fix_id_sequence.py --fix     # Fix the sequence")

if __name__ == "__main__":
    main()