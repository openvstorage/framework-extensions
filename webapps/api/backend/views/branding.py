# Copyright 2014 iNuron NV
#
# Licensed under the Open vStorage Modified Apache License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.openvstorage.org/license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Contains the BrandingViewSet
"""

from rest_framework import viewsets
from ovs.dal.lists.brandinglist import BrandingList
from ovs.dal.hybrids.branding import Branding
from backend.decorators import return_object, return_list, load, limit, log


class BrandingViewSet(viewsets.ViewSet):
    """
    Information about branding
    """
    prefix = r'branding'
    base_name = 'branding'

    @log()
    @limit(amount=60, per=60, timeout=60)
    @return_list(Branding)
    @load()
    def list(self):
        """
        Overview of all brandings
        """
        return BrandingList.get_brandings()

    @log()
    @limit(amount=60, per=60, timeout=60)
    @return_object(Branding)
    @load(Branding)
    def retrieve(self, branding):
        """
        Load information about a given branding
        """
        return branding
