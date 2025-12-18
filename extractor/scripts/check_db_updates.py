import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.models import DatabaseManager, Update
from datetime import datetime, timedelta

def check_updates():
    try:
        db = DatabaseManager()
        session = db.get_session()
        
        total_count = session.query(Update).count()
        print(f"Total updates in DB: {total_count}")
        
        yesterday = datetime.utcnow() - timedelta(days=1)
        recent_count = session.query(Update).filter(Update.created_at > yesterday).count()
        print(f"Updates created in last 24h: {recent_count}")
        
        last_5 = session.query(Update).order_by(Update.created_at.desc()).limit(5).all()
        print("\nLast 5 updates:")
        for u in last_5:
            print(f"- ID: {u.id}, Created: {u.created_at}, Title: {u.metadata_json.get('titulo', 'N/A')[:50]}")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    check_updates()
