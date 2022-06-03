import logging.config
import os

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'extended': {
            'format': (
                '%(asctime)s %(levelname)s [%(name)s]'
                '-[%(filename)s:%(lineno)d]'
                '-method:%(funcName)s-%(message)s'
            ),
            'datefmt': ('%Y-%m-%d %H:%M:%S')
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': os.environ.get("LOG_LEVEL","INFO"),
            'formatter': 'extended',
        }
    },
    'root': {
        'level': os.environ.get("LOG_LEVEL","INFO"),
        'handlers': ['console']
    }
}


logging.config.dictConfig(LOGGING)
logger = logging.getLogger("blade")
