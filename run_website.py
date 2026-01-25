# run_website.py
from app.plugins.website_module import WebsiteServer

if __name__ == '__main__':
    server = WebsiteServer(port=80)
    print("Starting website server in debug mode (blocking)...")
    server.start(debug=True)