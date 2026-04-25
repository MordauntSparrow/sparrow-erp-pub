from jinja2 import Environment, FileSystemLoader
import os
env = Environment(loader=FileSystemLoader(os.path.join(
    'app', 'plugins', 'website_module', 'templates', 'public')))
try:
    template = env.get_template('index.html')
    print('Template syntax is OK')
except Exception as e:
    print(f'Error: {e}')
