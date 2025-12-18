import os
import psycopg2
from dotenv import load_dotenv

# Load env vars from project root if possible
load_dotenv()

def verify_db_connection():
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        # Fallback to hardcoded for testing as seen in other files
        db_url = "postgresql://neondb_owner:npg_Tr1wXonS8EZy@ep-fragrant-feather-age2rjov-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require"
        print(f"⚠️  DATABASE_URL not found in env. using fallback: {db_url.split('@')[1]}")

    try:
        print("🔌 Connecting to database...")
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()
        
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        print(f"✅  Connected! DB Version: {version[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM updates;")
        count = cursor.fetchone()[0]
        print(f"✅  'updates' table access ok. Total records: {count}")

        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌  Connection failed: {e}")
        return False

if __name__ == "__main__":
    verify_db_connection()
