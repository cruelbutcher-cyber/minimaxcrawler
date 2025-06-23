#!/usr/bin/env python3
"""
Flask API Server for Web Crawler
RESTful API endpoints for the enhanced web crawler
"""

from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from flask_socketio import SocketIO, emit
import asyncio
import threading
import json
import uuid
from datetime import datetime
import os
import sys
from io import BytesIO

# Import our crawler backend
from web_crawler_backend import WebCrawlerAPI

# Flask app setup
app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
CORS(app, origins=['*'])  # Allow all origins for development
socketio = SocketIO(app, cors_allowed_origins="*")

# Global storage for active crawlers
active_crawlers = {}
crawler_threads = {}


class CrawlerManager:
    """Manages crawler instances and their threads"""
    
    def __init__(self):
        self.crawlers = {}
        self.threads = {}
    
    def create_crawler(self, crawler_id: str, start_url: str, crawl_mode: str):
        """Create a new crawler instance"""
        crawler = WebCrawlerAPI(start_url, crawl_mode)
        
        # Set up callbacks for real-time updates
        def on_progress(current, total):
            socketio.emit('progress_update', {
                'crawler_id': crawler_id,
                'current': current,
                'total': total,
                'percentage': (current / total * 100) if total > 0 else 0
            })
        
        def on_status(message):
            socketio.emit('status_update', {
                'crawler_id': crawler_id,
                'message': message,
                'timestamp': datetime.now().isoformat()
            })
        
        def on_results(results):
            socketio.emit('results_update', {
                'crawler_id': crawler_id,
                'results': results[-5:],  # Send last 5 results
                'total_count': len(results)
            })
        
        crawler.set_callbacks(on_progress, on_status, on_results)
        self.crawlers[crawler_id] = crawler
        return crawler
    
    def start_crawling(self, crawler_id: str):
        """Start crawling in a separate thread"""
        crawler = self.crawlers.get(crawler_id)
        if not crawler:
            return False
        
        def run_crawler():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(crawler.crawl_async())
            finally:
                loop.close()
                # Notify completion
                socketio.emit('crawl_completed', {
                    'crawler_id': crawler_id,
                    'results_count': len(crawler.results),
                    'timestamp': datetime.now().isoformat()
                })
        
        thread = threading.Thread(target=run_crawler, daemon=True)
        thread.start()
        self.threads[crawler_id] = thread
        return True
    
    def stop_crawler(self, crawler_id: str):
        """Stop a running crawler"""
        crawler = self.crawlers.get(crawler_id)
        if crawler:
            crawler.stop_crawling()
        return True
    
    def get_crawler(self, crawler_id: str):
        """Get crawler instance"""
        return self.crawlers.get(crawler_id)
    
    def remove_crawler(self, crawler_id: str):
        """Remove crawler and cleanup"""
        if crawler_id in self.crawlers:
            self.stop_crawler(crawler_id)
            del self.crawlers[crawler_id]
        if crawler_id in self.threads:
            del self.threads[crawler_id]


# Global crawler manager
crawler_manager = CrawlerManager()


@app.route('/')
def index():
    """API information endpoint"""
    return jsonify({
        'name': 'Enhanced Web Crawler API',
        'version': '1.0.0',
        'description': 'RESTful API for advanced web crawling and affiliate link detection',
        'endpoints': {
            'POST /api/crawlers': 'Create a new crawler',
            'GET /api/crawlers/{id}': 'Get crawler status',
            'POST /api/crawlers/{id}/start': 'Start crawling',
            'POST /api/crawlers/{id}/stop': 'Stop crawling',
            'GET /api/crawlers/{id}/results': 'Get crawler results',
            'GET /api/crawlers/{id}/download': 'Download results as CSV',
            'DELETE /api/crawlers/{id}': 'Delete crawler'
        }
    })


@app.route('/api/crawlers', methods=['POST'])
def create_crawler():
    """Create a new crawler instance"""
    try:
        data = request.get_json()
        
        if not data or 'start_url' not in data:
            return jsonify({'error': 'start_url is required'}), 400
        
        start_url = data['start_url']
        crawl_mode = data.get('crawl_mode', 'Standard')
        
        # Validate crawl_mode
        if crawl_mode not in ['Quick', 'Standard', 'Complete']:
            return jsonify({'error': 'Invalid crawl_mode. Must be Quick, Standard, or Complete'}), 400
        
        # Add https:// if not present
        if not start_url.startswith(('http://', 'https://')):
            start_url = f'https://{start_url}'
        
        # Generate unique crawler ID
        crawler_id = str(uuid.uuid4())
        
        # Create crawler
        crawler = crawler_manager.create_crawler(crawler_id, start_url, crawl_mode)
        
        return jsonify({
            'crawler_id': crawler_id,
            'start_url': start_url,
            'crawl_mode': crawl_mode,
            'max_pages': crawler.max_pages,
            'created_at': datetime.now().isoformat(),
            'status': 'created'
        }), 201
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/crawlers/<crawler_id>', methods=['GET'])
def get_crawler_status(crawler_id):
    """Get crawler status"""
    try:
        crawler = crawler_manager.get_crawler(crawler_id)
        if not crawler:
            return jsonify({'error': 'Crawler not found'}), 404
        
        status = crawler.get_status()
        return jsonify(status)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/crawlers/<crawler_id>/start', methods=['POST'])
def start_crawler(crawler_id):
    """Start crawling"""
    try:
        crawler = crawler_manager.get_crawler(crawler_id)
        if not crawler:
            return jsonify({'error': 'Crawler not found'}), 404
        
        if crawler.is_running:
            return jsonify({'error': 'Crawler is already running'}), 400
        
        success = crawler_manager.start_crawling(crawler_id)
        if success:
            return jsonify({
                'message': 'Crawling started',
                'crawler_id': crawler_id,
                'started_at': datetime.now().isoformat()
            })
        else:
            return jsonify({'error': 'Failed to start crawler'}), 500
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/crawlers/<crawler_id>/stop', methods=['POST'])
def stop_crawler(crawler_id):
    """Stop crawling"""
    try:
        crawler = crawler_manager.get_crawler(crawler_id)
        if not crawler:
            return jsonify({'error': 'Crawler not found'}), 404
        
        crawler_manager.stop_crawler(crawler_id)
        return jsonify({
            'message': 'Crawling stopped',
            'crawler_id': crawler_id,
            'stopped_at': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/crawlers/<crawler_id>/results', methods=['GET'])
def get_crawler_results(crawler_id):
    """Get crawler results"""
    try:
        crawler = crawler_manager.get_crawler(crawler_id)
        if not crawler:
            return jsonify({'error': 'Crawler not found'}), 404
        
        results = crawler.get_results()
        return jsonify(results)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/crawlers/<crawler_id>/download', methods=['GET'])
def download_results(crawler_id):
    """Download results as CSV"""
    try:
        crawler = crawler_manager.get_crawler(crawler_id)
        if not crawler:
            return jsonify({'error': 'Crawler not found'}), 404
        
        if not crawler.results:
            return jsonify({'error': 'No results to download'}), 404
        
        csv_data = crawler.generate_csv_data()
        
        # Create a BytesIO object
        output = BytesIO()
        output.write(csv_data.encode('utf-8'))
        output.seek(0)
        
        filename = f"crawl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        return send_file(
            output,
            mimetype='text/csv',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/crawlers/<crawler_id>', methods=['DELETE'])
def delete_crawler(crawler_id):
    """Delete crawler"""
    try:
        crawler = crawler_manager.get_crawler(crawler_id)
        if not crawler:
            return jsonify({'error': 'Crawler not found'}), 404
        
        crawler_manager.remove_crawler(crawler_id)
        return jsonify({
            'message': 'Crawler deleted',
            'crawler_id': crawler_id,
            'deleted_at': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/crawlers', methods=['GET'])
def list_crawlers():
    """List all active crawlers"""
    try:
        crawlers_info = []
        for crawler_id, crawler in crawler_manager.crawlers.items():
            status = crawler.get_status()
            crawlers_info.append({
                'crawler_id': crawler_id,
                'start_url': crawler.start_url,
                'crawl_mode': crawler.crawl_mode,
                'is_running': status['is_running'],
                'results_count': status['results_count'],
                'pages_crawled': status['pages_crawled']
            })
        
        return jsonify({
            'crawlers': crawlers_info,
            'total_count': len(crawlers_info)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# WebSocket event handlers
@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    print(f"Client connected: {request.sid}")
    emit('connected', {'message': 'Connected to Web Crawler API'})


@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print(f"Client disconnected: {request.sid}")


@socketio.on('subscribe_crawler')
def handle_subscribe_crawler(data):
    """Subscribe to crawler updates"""
    crawler_id = data.get('crawler_id')
    if crawler_id:
        # Join room for this crawler
        from flask_socketio import join_room
        join_room(crawler_id)
        emit('subscribed', {'crawler_id': crawler_id})


@socketio.on('unsubscribe_crawler')
def handle_unsubscribe_crawler(data):
    """Unsubscribe from crawler updates"""
    crawler_id = data.get('crawler_id')
    if crawler_id:
        # Leave room for this crawler
        from flask_socketio import leave_room
        leave_room(crawler_id)
        emit('unsubscribed', {'crawler_id': crawler_id})


# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    # Install required packages if not available
    try:
        import flask_cors
        import flask_socketio
    except ImportError:
        print("Installing required packages...")
        os.system("pip install flask flask-cors flask-socketio")
        print("Packages installed. Please restart the server.")
        sys.exit(1)
    
    print("Starting Enhanced Web Crawler API Server...")
    print("API Documentation available at: http://localhost:5000/")
    print("WebSocket connections available at: ws://localhost:5000/")
    
    # Run with SocketIO
    socketio.run(app, 
                host='0.0.0.0', 
                port=5000, 
                debug=True, 
                allow_unsafe_werkzeug=True)
