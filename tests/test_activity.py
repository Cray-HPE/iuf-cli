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
from unittest.mock import MagicMock, patch, Mock
import os

from lib.Activity import Activity, delete_activity, ActivityError
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

class DeleteActivityTest(unittest.TestCase):
    """Tests for the delete_activity function"""

    @patch('lib.Activity.shutil.rmtree')
    @patch('lib.Activity.os.path.exists')
    @patch('lib.ApiInterface.ApiInterface')
    def test_delete_activity_success_with_logs(self, mock_api_class, mock_exists, mock_rmtree):
        """Test successful deletion with log folder present"""
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "Activity deleted from backend"}
        mock_api.delete_activity.return_value = mock_response
        
        mock_exists.return_value = True
        
        success, message = delete_activity("test-activity")
        
        self.assertTrue(success)
        self.assertIn("Activity deleted from backend", message)
        self.assertIn("Deleted log folder", message)
        mock_api.delete_activity.assert_called_once_with("test-activity")
        mock_rmtree.assert_called_once_with("/etc/cray/upgrade/csm/iuf/test-activity")

    @patch('lib.Activity.shutil.rmtree')
    @patch('lib.Activity.os.path.exists')
    @patch('lib.ApiInterface.ApiInterface')
    def test_delete_activity_success_no_log_folder(self, mock_api_class, mock_exists, mock_rmtree):
        """Test successful deletion when log folder doesn't exist"""
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "Activity deleted"}
        mock_api.delete_activity.return_value = mock_response
        
        mock_exists.return_value = False
        
        success, message = delete_activity("test-activity")
        
        self.assertTrue(success)
        self.assertIn("does not exist", message)
        mock_rmtree.assert_not_called()

    @patch('lib.Activity.shutil.rmtree')
    @patch('lib.Activity.os.path.exists')
    @patch('lib.ApiInterface.ApiInterface')
    def test_delete_activity_log_deletion_fails(self, mock_api_class, mock_exists, mock_rmtree):
        """Test deletion when log folder removal fails with any exception"""
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"message": "Activity deleted"}
        mock_api.delete_activity.return_value = mock_response
        
        mock_exists.return_value = True
        mock_rmtree.side_effect = OSError("Disk error")
        
        success, message = delete_activity("test-activity")
        
        self.assertTrue(success)
        self.assertIn("Warning", message)
        self.assertIn("Manual cleanup required", message)
        self.assertIn("sudo rm -rf", message)

    @patch('lib.ApiInterface.ApiInterface')
    def test_delete_activity_backend_returns_error(self, mock_api_class):
        """Test deletion when backend returns non-200 status"""
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal server error"
        mock_response.json.return_value = {"message": "Backend error occurred"}
        mock_api.delete_activity.return_value = mock_response
        
        with self.assertRaises(ActivityError) as context:
            delete_activity("test-activity")
        
        self.assertIn("Backend error occurred", str(context.exception))

    @patch('lib.ApiInterface.ApiInterface')
    def test_delete_activity_backend_raises_exception(self, mock_api_class):
        """Test deletion when API call raises an exception"""
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        mock_api.delete_activity.side_effect = ConnectionError("Network error")
        
        with self.assertRaises(ActivityError) as context:
            delete_activity("test-activity")
        
        error_msg = str(context.exception)
        self.assertIn("test-activity", error_msg)

    @patch('lib.ApiInterface.ApiInterface')
    def test_delete_activity_backend_returns_404(self, mock_api_class):
        """Test deletion of non-existent activity"""
        mock_api = Mock()
        mock_api_class.return_value = mock_api
        
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Activity not found"
        mock_response.json.return_value = {"message": "Activity not found"}
        mock_api.delete_activity.return_value = mock_response
        
        with self.assertRaises(ActivityError) as context:
            delete_activity("non-existent-activity")
        
        self.assertIn("Activity not found", str(context.exception))

if __name__ == '__main__':
    unittest.main()
