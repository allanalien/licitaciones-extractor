#!/usr/bin/env python3
"""
Railway Cron Job Script for Daily Extraction
Ejecuta la extracción de licitaciones diariamente
"""

import os
import sys
import logging
from datetime import datetime
from main_extractor import main

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'logs/cron_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)

logger = logging.getLogger(__name__)

def run_daily_extraction():
    """Execute daily extraction job"""
    try:
        logger.info("=" * 60)
        logger.info(f"Starting daily extraction at {datetime.now()}")
        logger.info("=" * 60)

        # Run main extraction
        result = main()

        if result:
            logger.info("Daily extraction completed successfully")
        else:
            logger.error("Daily extraction completed with errors")

        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Fatal error in daily extraction: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_daily_extraction()