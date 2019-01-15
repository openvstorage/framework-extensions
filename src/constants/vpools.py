import os

VPOOL_BASE_PATH = os.path.join(os.path.sep, 'ovs', 'vpools', '{0}')

# MDS RELATED
MDS_CONFIG_PATH = os.path.join(VPOOL_BASE_PATH, 'mds_config')           # ovs/vpools/{0}/mds_config

# PROXY RELATED
PROXY_BASE_PATH = os.path.join(VPOOL_BASE_PATH, 'proxies')              # ovs/vpools/{0}/proxies
PROXY_PATH = os.path.join(PROXY_BASE_PATH, '{1}')                       # ovs/vpools/{0}/proxies/{1}
PROXY_CONFIG_PATH = os.path.join(PROXY_PATH, 'config')                  # ovs/vpools/{0}/proxies/{1}/config
PROXY_CONFIG_ABM = os.path.join(PROXY_CONFIG_PATH, 'abm.ini')           # ovs/vpools/{0}/proxies/{1}/config/abm.ini
PROXY_CONFIG_MAIN = os.path.join(PROXY_CONFIG_PATH, 'main')             # ovs/vpools/{0}/proxies/{1}/config/main

SCRUB_BASE_PATH = os.path.join(PROXY_BASE_PATH, 'scrub')                # ovs/vpools/{0}/proxies/scrub
GENERIC_SCRUB = os.path.join(SCRUB_BASE_PATH, 'generic_scrub')          # ovs/vpools/{0}/proxies/scrub/generic_scrub

# HOSTS RELATED
HOSTS_BASE_PATH = os.path.join(VPOOL_BASE_PATH, 'hosts')                # /ovs/vpools/{0}/hosts
HOSTS_PATH = os.path.join(HOSTS_BASE_PATH, '{1}')                       # /ovs/vpools/{0}/hosts/{1}
HOSTS_CONFIG_PATH = os.path.join(HOSTS_PATH, 'config')                  # /ovs/vpools/{0}/hosts/{1}/config
