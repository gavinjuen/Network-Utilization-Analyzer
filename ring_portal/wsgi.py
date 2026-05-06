import os
from django.core.wsgi import get_wsgi_application
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ring_portal.settings")
application = get_wsgi_application()

os.environ["SECRET_KEY"] = "your-secret-key-here"
os.environ["DEBUG"] = "False"
os.environ["ALLOWED_HOSTS"] = "gavinjuen.pythonanywhere.com"
os.environ["CSRF_TRUSTED_ORIGINS"] = "https://gavinjuen.pythonanywhere.com"
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "ring_portal.settings")