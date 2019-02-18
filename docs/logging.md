# Logging

The extensions library uses the `ovs_extensions` logger through the application.
This logger can be configured to suit your needs.

A simple default configuration can be loaded by:
```
from ovs_extensions.log import configure_logger_with_recommended_settings

configure_logger_with_recommended_settings()
```
This will set the default config:
```
{'disable_existing_loggers': False,
 'formatters': {'ovs': {'()': 'ovs_extensions.log.LogFormatter',
                        'format': '%(asctime)s - %(hostname)s - %(process)s/%(thread)d - %(name)s - %(funcName)s - %(sequence)s - %(levelname)s - %(message)s'},
                'standard': {'format': '%(asctime)s [%(levelname)s] %(name)s: %(message)s'}},
 'handlers': {'default': {'class': 'logging.StreamHandler',
                          'formatter': 'ovs',
                          'level': 'INFO'}},
 'loggers': {'ovs_extensions': {'handlers': ['default'],
                                'level': 'INFO',
                                'propagate': True},
             'paramiko': {'level': 'WARNING'},
             'requests': {'level': 'WARNING'},
             'urllib3': {'level': 'WARNING'}},
 'version': 1}
```
Note: the paramiko, requests and urllib3 loggers are set to WARNING also!
