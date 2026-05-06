import os
import sys

project_home = "/home/gavinjuen/Network-Utilization-Analyzer"
if project_home not in sys.path:
    sys.path.insert(0, project_home)

os.environ["SECRET_KEY"] = "your-secret-key-here"
os.environ["DEBUG"] = "False"
os.environ["ALLOWED_HOSTS"] = "gavinjuen.pythonanywhere.com"
os.environ["CSRF_TRUSTED_ORIGINS"] = "https://gavinjuen.pythonanywhere.com"
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ring_portal.settings")

from django.core.wsgi import get_wsgi_application
application = get_wsgi_application()