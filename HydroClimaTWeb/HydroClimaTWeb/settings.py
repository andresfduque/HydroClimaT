"""
Common settings and globals.
"""

import random
import os

# ======================================================================================================================
# BASE
# ======================================================================================================================

# **********************************************************************************************************************
# NAMES CONFIGURATION
APP_NAME = "odm2admin"          # This has to match the name of the folder that the app is saved
VERBOSE_NAME = "ODM2 Admin"

SITE_HEADER = "ODM2 Administration"
SITE_TITLE = "ODM2 Administration"
# End of names configuration

# **********************************************************************************************************************
# PATH CONFIGURATION
BASE_DIR = os.path.dirname(os.path.dirname(__file__))   # Absolute filesystem path to this Django project directory
# Absolute path where project is located
ROOT = os.path.dirname(BASE_DIR)                        # '/SHDD/Personal Projects/01-Programs Development/\
# PycharmProjects/HydroClimaT/HydroClimaTWeb/HydroClimaTWeb/'

# Secret Key
SECRET_KEY = '=g3@%42m0f%5$jhl1b0cu)ziqj@i6d4s=axjyd=ii+%llop47w'
# Application definition
BASE_URL = '' # Enter the base url in your APACHE SETTINGS. e.g. 'ODM2ADMIN/'

CUSTOM_TEMPLATE_PATH = '/{}{}/'.format(BASE_URL, APP_NAME)
# End of path configuration

# **********************************************************************************************************************
# DEBUG CONFIGURATION
DEBUG = True    # Disable debugging by default
# End of debug configuration

ALLOWED_HOSTS = ['127.0.0.1',
                 ]

# **********************************************************************************************************************
# TEMPLATE CONFIGURATION
TEMPLATE_DIR = os.path.join(ROOT)
TEMPLATE_PATH = os.path.join(TEMPLATE_DIR, 'templatesAndSettings/templates')

# List of callable that know how to import templates from various sources.
TEMPLATES = [{
    'BACKEND': 'django.template.backends.django.DjangoTemplates',
    'DIRS': [TEMPLATE_PATH, ],
    'APP_DIRS': True,
    'OPTIONS': {
        # 'loaders': [(
        #             'django.template.loaders.filesystem.Loader',
        #             'django.template.loaders.app_directories.Loader',
        #             'apptemplates.Loader',
        #             ), ],
        'debug': DEBUG,
        'context_processors': [
            'django.template.context_processors.media',
            'django.template.context_processors.static',
            'django.template.context_processors.debug',
            'django.template.context_processors.request',
            'django.contrib.auth.context_processors.auth',
            'django.contrib.messages.context_processors.messages',
            'social_django.context_processors.backends',
            'social_django.context_processors.login_redirect',
        ],
    },
}]
# End of template configuration

# **********************************************************************************************************************
# MANAGER CONFIGURATION

# Admin and managers for this project. These people receive private site alerts
ADMINS = [
    {"name": "first last",
     "email": "email@example.com"}
]
# End of manager configuration


# **********************************************************************************************************************
# GENERAL CONFIGURATION

# Local time zone for this installation. Choices can be found here:
# http://en.wikipedia.org/wiki/List_of_tz_zones_by_name although not all
# choices may be available on all operating systems. On Unix systems, a value
# of None will cause Django to use the same timezone as the operating system.
# If running in a Windows environment this must be set to the same as your
# system time zone.
TIME_ZONE = 'UTC'

# Language code for this installation. All choices can be found here:
# http://www.i18nguy.com/unicode/language-identifiers.html.
LANGUAGE_CODE = 'en-us'

# If you set this to False, Django will make some optimizations so as not to load the internationalization machinery
USE_I18N = True

# If you set this to False, Django will not format dates, numbers and calendars according to the current locale
USE_L10N = True

# Time zone support is disabled by default. To enable it, set USE_TZ = True
USE_TZ = True

UTC_OFFSET = -5
# End of general configuration

# **********************************************************************************************************************
# MEDIA CONFIGURATION

# Absolute filesystem path to the directory that will hold user-uploaded files.
MEDIA_ROOT = '{}/{}/upfiles/'.format(BASE_DIR, APP_NAME)
#  URL that handles the media served from MEDIA_ROOT.
MEDIA_URL = '/{}/{}/media/'.format(os.path.basename(BASE_DIR), APP_NAME)
# Absolute filesystem path to the directory that will hold database export and import files
FIXTURE_DIR = '{}/{}/fixtures/'.format(BASE_DIR, APP_NAME)
# End of media configuration

# **********************************************************************************************************************
# STATIC FILE CONFIGURATION

# Absolute path to the directory static files should be collected to. Don't put
# anything in this directory yourself; store your static files in apps' static/
# subdirectories and in STATICFILES_DIRS.
STATIC_ROOT = '{}/{}/static'.format(BASE_DIR, APP_NAME)
# URL prefix for static files.
STATIC_URL = '/static/'
# End of static file configuration
# End of path configuration

# **********************************************************************************************************************
# MIDDLEWARE CONFIGURATION
MIDDLEWARE = (
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'social_django.middleware.SocialAuthExceptionMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
)
# End of middleware configuration

# **********************************************************************************************************************
# OAUTH SETTINGS
AUTHENTICATION_BACKENDS = (
    'social_core.backends.open_id.OpenIdAuth',
    'social_core.backends.google.GoogleOpenId',
    'social_core.backends.google.GoogleOAuth2',
    #'odm2admin.hydroshare_backend.HydroShareOAuth2',
    'social_core.backends.google.GoogleOAuth',
    'django.contrib.auth.backends.ModelBackend',
)
# Oauth CORS_ORIGIN_ALLOW_ALL = True

SOCIAL_AUTH_URL_NAMESPACE = 'social'
SOCIAL_AUTH_PIPELINE = (
    'social_core.pipeline.social_auth.social_details',
    'social_core.pipeline.social_auth.social_uid',
    'social_core.pipeline.social_auth.social_user',
    'social_core.pipeline.user.get_username',
    'social_core.pipeline.user.create_user',
    'social_core.pipeline.social_auth.associate_user',
    'social_core.pipeline.social_auth.load_extra_data',
    'social_core.pipeline.user.user_details',
    'social_core.pipeline.social_auth.associate_by_email',
)
# End of oauth configuration

# **********************************************************************************************************************
# URL AND WSGI CONFIGURATION
ROOT_URLCONF = 'templatesAndSettings.urls'
WSGI_APPLICATION = 'templatesAndSettings.wsgi.application'
# End of oauth configuration

# **********************************************************************************************************************
# APP CONFIGURATION
INSTALLED_APPS = (
    'jquery',
    'djangocms_admin_style',
    '{}'.format(APP_NAME),
    'import_export',
    'social_django',
    'admin_shortcuts',
    'daterange_filter',
    'captcha',
    'fixture_magic',
    'ajax_select',
    'django.contrib.admin',
    'django.contrib.gis',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',

)
# End of app configuration

# **********************************************************************************************************************
# ADMIN SHORTCUTS CONFIGURATION
ADMIN_SHORTCUTS = [
    {

        'shortcuts': [
            {
                'url': CUSTOM_TEMPLATE_PATH,
                'app_name': '{}'.format(APP_NAME),
                'title': '{}'.format(VERBOSE_NAME),
                'class': 'config',
            },
            {
                'url': '/' + 'AddSensor',
                'app_name': '{}'.format(APP_NAME),
                'title': 'Add Sensor Data',
                'class': 'tool',
            },
            {
                'url': '/' + 'AddProfile',
                'app_name': '{}'.format(APP_NAME),
                'title': 'Add Soil Profile Data',
                'class': 'flag',
            },
            {
                'url': '/' + 'RecordAction',
                'app_name': '{}'.format(APP_NAME),
                'title': 'Record an Action',
                'class': 'notepad',
            },
            {
                'url': '/' + 'ManageCitations',
                'app_name': '{}'.format(APP_NAME),
                'title': 'Manage Citations',
                'class': 'pencil',
            },
            {
                'url': '/' + 'chartIndex',
                'app_name': '{}'.format(APP_NAME),
                'title': 'Graph My Data',
                'class': 'monitor',
            },
        ]
    },
]
ADMIN_SHORTCUTS_SETTINGS = {
    'hide_app_list': False,
    'open_new_window': False,
}
# End of admin shortcuts configuration

# **********************************************************************************************************************
# SAMPLING FEATURE TYPE LEGEND MAPPING

LEGEND_MAP = {
        'Excavation': dict(feature_type="Excavation", icon="fa-spoon", color="darkred",
                           style_class="awesome-marker-icon-darkred"),
        'Field area': dict(feature_type="Field area", icon="fa-map-o", color="darkblue",
                           style_class="awesome-marker-icon-darkblue"),
        'Weather station': dict(feature_type="Weather station", icon="fa-cloud", color="darkblue",
                                style_class="awesome-marker-icon-darkblue"),
        'Ecological land classification': dict(feature_type="Ecological land classification",
                                               icon="fa-bar-chart", color="darkpurple",
                                               style_class="awesome-marker-icon-darkpurple"),
        'Observation well': dict(feature_type="Observation well", icon="fa-eye", color="orange",
                                 style_class="awesome-marker-icon-orange"),
        'Site': dict(feature_type="Site", icon="fa-dot-circle-o", color="green",
                     style_class="awesome-marker-icon-green"),
        'Stream gage': dict(feature_type="Stream gage", icon="fa-tint", color="blue",
                            style_class="awesome-marker-icon-blue"),
        'Transect': dict(feature_type="Transect", icon="fa-area-chart", color="cadetblue",
                         style_class="awesome-marker-icon-cadetblue"),
        'Profile': dict(feature_type="Profile", icon="fa-database", color="purple",
             style_class="awesome-marker-icon-purple"),
        'Specimen': dict(feature_type="Specimen", icon="fa-flask", color="cadetblue",
                         style_class="awesome-marker-icon-cadetblue")
    }
# End of sampling feature type legend mapping

# **********************************************************************************************************************
# REDIS CACHING CONFIGURATION
CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient"
        },
        "KEY_PREFIX": "odm2admin"
    }
}

CACHE_TTL = 60 * 15     # Cache time to live is 15 minutes

# End of redis caching configuration

# ======================================================================================================================
# DEVELOPMENT
# ======================================================================================================================

""" EXPORTDB FLAG CONFIGURATION - if set to true this will use Camel case table names for SQLite"""
EXPORTDB = False
""" EXPORTDB FLAG CONFIGURATION """

""" TRAVIS CONFIGURATION """
TRAVIS_ENVIRONMENT = False
if 'TRAVIS' in os.environ:
    TRAVIS_ENVIRONMENT = True
""" END TRAVIS CONFIGURATION """

""" ALLOWED HOSTS CONFIGURATION """
ALLOWED_HOSTS = ['127.0.0.1',]
""" END ALLOWED HOSTS CONFIGURATION """


""" EMAIL CONFIGURATION """
EMAIL_HOST = 'smtp.host'
EMAIL_HOST_USER = 'user'
EMAIL_HOST_PASSWORD = 'password'
EMAIL_FROM_ADDRESS = 'do-not-reply-ODM2-Admin@cuahsi.org'
RECAPTCHA_PUBLIC_KEY = 'googlerecaptchakey'
RECAPTCHA_PRIVATE_KEY = 'googlerecaptchaprivatekey'
EMAIL_USE_TLS = True
EMAIL_PORT = 123
""" EMAIL CONFIGURATION """

""" DATABASE CONFIGURATION """
if TRAVIS_ENVIRONMENT:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': 'test',  # Must match travis.yml setting
            'USER': 'postgres',
            'PASSWORD': '',
            'HOST': 'localhost',
            'PORT': '',
        }
    }
else:

    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': 'odm_col',
            'USER': 'postgres',
            'PASSWORD': 'postgres',
            'HOST': 'localhost',
            'PORT': '5432',
            'OPTIONS': {
                'options': '-c search_path=public,admin,odm2,odm2extra'
            }
        }
    }
""" END DATABASE CONFIGURATION """

""" SENSOR DASHBOARD CONFIGURATION """

SENSOR_DASHBOARD = {
    "time_series_days": 30,
    "featureactionids": [1699, 1784,1782,1701],
}
""" END SENSOR DASHBOARD CONFIGURATION"""

""" MAP CONFIGURATION """
MAP_CONFIG = {
    "lat": 0,
    "lon": 0,
    "zoom": 2,
    "cluster_feature_types": ['Profile','Specimen','Excavation','Field area'],
    "time_series_months": 1,
    "display_titles": True,
    "MapBox": {
      "access_token": 'mapbox accessToken'
    },
    "result_value_processing_levels_to_display": [1, 2, 3],
    "feature_types": ['Site','Profile','Specimen','Excavation','Field area',
                  'Weather station','Observation well','Stream gage','Transect']
}
""" END MAP CONFIGURATION """


""" DATA DISCLAIMER CONFIGURATION """
DATA_DISCLAIMER = {
    "text" : "Add a link discribing where your data come from",
    "linktext" : "The name of my site",
    "link" : "http://mysiteswegpage.page/",
}
""" END DATA DISCLAIMER CONFIGURATION """