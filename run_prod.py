from waitress import serve
from app import app
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('waitress')

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    host = '0.0.0.0'  # Listen on all interfaces for internal network access
    
    logger.info(f"Starting production server on http://{host}:{port}")
    serve(app, host=host, port=port, threads=8)
