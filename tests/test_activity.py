#
# MIT License
#
# (C) Copyright 2023 Hewlett Packard Enterprise Development LP
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#

import unittest
from unittest.mock import MagicMock

from lib.Activity import Activity
from lib.SiteConfig import SiteConfig


class ActivityBootprepCommandsTest(unittest.TestCase):
    """Tests for the bootprep_commands property of Activity"""

    def setUp(self):
        mock_activity = MagicMock(spec=Activity)
        mock_activity.site_conf = MagicMock(spec=SiteConfig)
        self.mock_activity = mock_activity

    def test_non_empty_bootprep_commands(self):
        self.mock_activity.site_conf.bootprep_commands = [
            'cd /etc/cray/upgrade/csm',
            'sat bootprep run --limit images --limit session_templates --overwrite-images \\\n'
            '    --overwrite-templates --vars-file "session_vars.yaml" --format json \\\n'
            '    --bos-version v2 bootprep-managed.yaml'
        ]
        expected = (
            '    cd /etc/cray/upgrade/csm\n'
            '    sat bootprep run --limit images --limit session_templates --overwrite-images \\\n'
            '        --overwrite-templates --vars-file "session_vars.yaml" --format json \\\n'
            '        --bos-version v2 bootprep-managed.yaml'
        )
        self.assertEqual(expected, Activity.bootprep_commands.__get__(self.mock_activity))


if __name__ == '__main__':
    unittest.main()
