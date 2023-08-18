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

import functools
import unittest
from unittest.mock import MagicMock

from lib.SiteConfig import SiteConfig


class SiteConfigBootprepCommandsTest(unittest.TestCase):
    """Tests for the update_bootprep_commands method of SiteConfig"""

    def setUp(self):
        self.media_dir = '/etc/cray/upgrade/csm'
        self.relative_bpc_managed = 'managed-bootprep.yaml'
        self.relative_bpc_management = 'management-bootprep.yaml'

        mock_site_config = MagicMock()
        mock_site_config.update_bootprep_commands = functools.partial(SiteConfig.update_bootprep_commands,
                                                                      mock_site_config)
        mock_site_config.sat_commands = []
        mock_site_config.media_dir = self.media_dir
        mock_site_config.relative_bpc_managed = None
        mock_site_config.relative_bpc_management = None

        self.mock_site_config = mock_site_config

    def test_non_bootprep_stage(self):
        """Test that a non-bootprep stage has no sat commands"""
        self.mock_site_config.update_bootprep_commands('deliver-product')
        self.assertEqual([], self.mock_site_config.sat_commands)

    def test_bootprep_stage_but_no_bootprep_files(self):
        """Test that a bootprep stage without bootprep files results in no sat commands"""
        self.mock_site_config.relative_bpc_managed = None
        self.mock_site_config.relative_bpc_management = None
        self.mock_site_config.update_bootprep_commands('update-cfs-config')
        self.mock_site_config.update_bootprep_commands('prepare-images')
        self.assertEqual([], self.mock_site_config.sat_commands)

    def test_update_cfs_config_stage(self):
        """Test that update_cfs_config with a bootprep file results in a sat command"""
        self.mock_site_config.relative_bpc_managed = self.relative_bpc_managed
        self.mock_site_config.update_bootprep_commands('update-cfs-config')

        expected_commands = [
            f'cd {self.media_dir}',
            f'sat bootprep run --limit configurations --overwrite-configs --vars-file \\\n'
            f'    "session_vars.yaml" --format json --bos-version v2 {self.relative_bpc_managed}'
        ]

        self.assertEqual(expected_commands, self.mock_site_config.sat_commands)

    def test_prepare_images_stage(self):
        """Test that prepare_images with a bootprep_file results in a sat command"""
        self.mock_site_config.relative_bpc_managed = self.relative_bpc_managed
        self.mock_site_config.update_bootprep_commands('prepare-images')

        expected_commands = [
            f'cd {self.media_dir}',
            f'sat bootprep run --limit images --limit session_templates --overwrite-images \\\n'
            f'    --overwrite-templates --vars-file "session_vars.yaml" --format json \\\n'
            f'    --bos-version v2 {self.relative_bpc_managed}'
        ]

        self.assertEqual(expected_commands, self.mock_site_config.sat_commands)

    def test_multiple_stages_multiple_bootprep_files(self):
        """Test that multiple bootprep stages with multiple bootprep files results in multiple commands."""
        self.mock_site_config.relative_bpc_managed = self.relative_bpc_managed
        self.mock_site_config.relative_bpc_management = self.relative_bpc_management

        # These two stages should add nothing
        self.mock_site_config.update_bootprep_commands('deliver-product')
        self.mock_site_config.update_bootprep_commands('update-vcs-config')
        # These two stages should add a cd command and two bootprep commands each
        self.mock_site_config.update_bootprep_commands('update-cfs-config')
        self.mock_site_config.update_bootprep_commands('prepare-images')

        expected_commands = [
            f'cd {self.media_dir}',
            f'sat bootprep run --limit configurations --overwrite-configs --vars-file \\\n'
            f'    "session_vars.yaml" --format json --bos-version v2 \\\n'
            f'    {self.relative_bpc_management}',
            f'sat bootprep run --limit configurations --overwrite-configs --vars-file \\\n'
            f'    "session_vars.yaml" --format json --bos-version v2 {self.relative_bpc_managed}',
            f'sat bootprep run --limit images --limit session_templates --overwrite-images \\\n'
            f'    --overwrite-templates --vars-file "session_vars.yaml" --format json \\\n'
            f'    --bos-version v2 {self.relative_bpc_management}',
            f'sat bootprep run --limit images --limit session_templates --overwrite-images \\\n'
            f'    --overwrite-templates --vars-file "session_vars.yaml" --format json \\\n'
            f'    --bos-version v2 {self.relative_bpc_managed}'
        ]

        self.assertEqual(expected_commands, self.mock_site_config.sat_commands)


if __name__ == '__main__':
    unittest.main()
