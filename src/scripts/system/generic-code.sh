#!/usr/bin/env bash
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


function show_generic_help() {
    python -c "from ovs_extensions.scripts.system.print_templates import print_miscellaneous_cli; print_miscellaneous_cli('$0') "
    python -c "from ovs_extensions.scripts.system.print_templates import print_unittest_cli; print_unittest_cli('$0') "
}

function test_unittest() {
    if [ "$#" -eq 1 ]; then
        python -c "from ovs_extensions.generic.unittests import UnitTest; UnitTest('$0').run_tests()"
    elif [ "$2" = "list" ] && [ "$#" -eq 2 ]; then
        python -c "from ovs_extensions.generic.unittests import UnitTest; UnitTest('$0').list_tests(print_tests=True)"
    elif [ "$2" = "--averages" ] && [ "$#" -eq 2 ]; then
        python -c "from ovs_extensions.generic.unittests import UnitTest; UnitTest('$0').run_tests(add_averages=True)"
    elif [ "$#" -ge 4 ]; then
        show_help
    else
        if [[ "$2" == *.py ]]; then
            directory=$(dirname "$2")
            filename=$(basename "$2")
            filename="${filename%.*}"
            filename="$directory"/"$filename"
        else
            filename="$2"
        fi
        if [ "$#" -eq 3 ] && [ "$3" != "--averages" ]; then
            show_help
        else
            if [ "$3" = "--averages" ]; then
                python -c "from ovs_extensions.generic.unittests import UnitTest; UnitTest('$0').run_tests('$filename', add_averages=True)"
            else
                python -c "from ovs_extensions.generic.unittests import UnitTest; UnitTest('$0').run_tests('$filename', add_averages=False)"
            fi
        fi
    fi
}