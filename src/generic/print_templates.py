# Copyright (C) 2016 iNuron NV
#
# This file is part of Open vStorage Open Source Edition (OSE),
# as available from
#
#      http://www.openvstorage.org and
#      http://www.openvstorage.com.
#
# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License v3 (GNU AGPLv3)
# as published by the Free Software Foundation, in version 3 as it comes
# in the LICENSE.txt file of the Open vStorage OSE distribution.
#
# Open vStorage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY of any kind.

def _get_mgr_from_path(path):
    return path.split('/')[-1]

def print_unittest_cli(path):
    """
    Print the generic Unittest cli options
    :param path: /usr/lib/ovs, /usr/lib/iscsiadm
    :return:
    """
    mgr = _get_mgr_from_path(path)
    print "  * Unit tests:\n" \
          "    - {0} unittest [--averages]                  Execute all unittests (and optionally add some averages)\n" \
          "    - {0} unittest list                          List all unittests\n" \
          "    - {0} unittest <filepath> [--averages]       Run the unittests in <filepath> (and optionally add some averages)\n" \
          "    - {0} unittest <modulename> [--averages]     Run the unittests in <module> (and optionally add some averages)\n" \
          "                                      Both filepath and modulenames can be passed in a comma-separated list\n" \
          "                                      e.g. ovs unittest module1, module2, module3".format(mgr)


def print_miscellaneous_cli(path):
    mgr = _get_mgr_from_path(path)
    print "Usage:\n" \
          "  * Miscellaneous options:\n" \
          "    - {0}                       Launch {0} shell (ipython)\n" \
          "    - {0} help                  Show this help section\n" \
          "    - {0} collect logs          Collect all {0} logs to a tarball (for support purposes)\n".format(mgr)
