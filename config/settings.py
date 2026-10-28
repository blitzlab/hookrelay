"""Django settings for hookrelay.

Every value that differs between environments is read from the process
environment so the same image runs locally, in CI and in production.
"""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent


def env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def env_bool(name: str, default: bool = False) -> bool:
    return env(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    return int(env(name, str(default)))


def env_float(name: str, default: float) -> float:
    return float(env(name, str(default)))


SECRET_KEY = env("DJANGO_SECRET_KEY", "dev-insecure-change-me")
DEBUG = env_bool("DJANGO_DEBUG", True)
ALLOWED_HOSTS = [h.strip() for h in env("DJANGO_ALLOWED_HOSTS", "*").split(",") if h.strip()]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "apps.webhooks",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": env("POSTGRES_DB", "hookrelay"),
        "USER": env("POSTGRES_USER", "hookrelay"),
        "PASSWORD": env("POSTGRES_PASSWORD", "hookrelay"),
        "HOST": env("POSTGRES_HOST", "localhost"),
        "PORT": env("POSTGRES_PORT", "5432"),
        "CONN_MAX_AGE": env_int("POSTGRES_CONN_MAX_AGE", 60),
    }
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

REST_FRAMEWORK = {
    "DEFAULT_AUTHENTICATION_CLASSES": [],
    "DEFAULT_PERMISSION_CLASSES": ["apps.webhooks.permissions.ApiKeyPermission"],
    "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.LimitOffsetPagination",
    "PAGE_SIZE": 50,
    "UNAUTHENTICATED_USER": None,
}

# Redis holds shared, short-lived coordination state (circuit breakers).
REDIS_URL = env("REDIS_URL", "redis://localhost:6379/1")

# Celery is only a trigger. PostgreSQL stays the source of truth for every
# delivery, so a lost broker message delays a webhook but never drops it.
CELERY_BROKER_URL = env("CELERY_BROKER_URL", "redis://localhost:6379/0")
CELERY_TASK_IGNORE_RESULT = True
CELERY_TASK_ACKS_LATE = True
CELERY_TASK_REJECT_ON_WORKER_LOST = True
CELERY_WORKER_PREFETCH_MULTIPLIER = 1
CELERY_TASK_DEFAULT_QUEUE = "default"
CELERY_TASK_ROUTES = {
    "apps.webhooks.tasks.relay_outbox": {"queue": "outbox"},
    "apps.webhooks.tasks.sweep_stale_deliveries": {"queue": "outbox"},
    "apps.webhooks.tasks.deliver": {"queue": "deliveries"},
}
# Retries are scheduled with countdowns of up to BACKOFF_CAP_SECONDS. The Redis
# transport redelivers unacked messages after visibility_timeout, so it must be
# longer than the longest countdown to avoid needless duplicate attempts.
CELERY_BROKER_TRANSPORT_OPTIONS = {"visibility_timeout": env_int("CELERY_VISIBILITY_TIMEOUT", 7200)}
CELERY_BEAT_SCHEDULE = {
    "relay-outbox": {
        "task": "apps.webhooks.tasks.relay_outbox",
        "schedule": env_float("HOOKRELAY_OUTBOX_POLL_SECONDS", 1.0),
    },
    "sweep-stale-deliveries": {
        "task": "apps.webhooks.tasks.sweep_stale_deliveries",
        "schedule": env_float("HOOKRELAY_SWEEP_INTERVAL_SECONDS", 30.0),
    },
}

HOOKRELAY = {
    # Shared secret for the management API. Empty disables auth (local dev only).
    "API_KEY": env("HOOKRELAY_API_KEY", ""),
    "MAX_ATTEMPTS": env_int("HOOKRELAY_MAX_ATTEMPTS", 8),
    "BACKOFF_BASE_SECONDS": env_float("HOOKRELAY_BACKOFF_BASE_SECONDS", 2.0),
    "BACKOFF_CAP_SECONDS": env_float("HOOKRELAY_BACKOFF_CAP_SECONDS", 3600.0),
    "REQUEST_TIMEOUT_SECONDS": env_float("HOOKRELAY_REQUEST_TIMEOUT_SECONDS", 10.0),
    "OUTBOX_BATCH_SIZE": env_int("HOOKRELAY_OUTBOX_BATCH_SIZE", 100),
    "DELIVERY_LEASE_SECONDS": env_int("HOOKRELAY_DELIVERY_LEASE_SECONDS", 120),
    "SWEEP_GRACE_SECONDS": env_int("HOOKRELAY_SWEEP_GRACE_SECONDS", 60),
    "BREAKER_FAILURE_THRESHOLD": env_int("HOOKRELAY_BREAKER_FAILURE_THRESHOLD", 5),
    "BREAKER_WINDOW_SECONDS": env_int("HOOKRELAY_BREAKER_WINDOW_SECONDS", 60),
    "BREAKER_COOLDOWN_SECONDS": env_float("HOOKRELAY_BREAKER_COOLDOWN_SECONDS", 30.0),
    "BREAKER_MAX_COOLDOWN_SECONDS": env_float("HOOKRELAY_BREAKER_MAX_COOLDOWN_SECONDS", 900.0),
    # Block endpoints that resolve to private or loopback addresses (SSRF guard).
    "ALLOW_PRIVATE_URLS": env_bool("HOOKRELAY_ALLOW_PRIVATE_URLS", DEBUG),
}

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "kv": {"format": "ts=%(asctime)s level=%(levelname)s logger=%(name)s msg=%(message)s"},
    },
    "handlers": {"console": {"class": "logging.StreamHandler", "formatter": "kv"}},
    "root": {"handlers": ["console"], "level": env("LOG_LEVEL", "INFO")},
}
