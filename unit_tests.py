#!/usr/bin/env python3

import unittest
from unittest.mock import Mock, patch

import sci

# just an example - we need more


class GetToolboxTestCase(unittest.TestCase):
    @patch("sci.fh.requests")
    def test_getToolbox(self, mock_requests):
        mock = Mock()
        mock.raise_for_status.return_value = None
        mock.json.return_value = '{"foo": "bar"}'
        mock_requests.get.return_value = mock
        ret = sci.fh.getToolbox("somefile.json")
        ret = sci.fh.getToolbox("somefile.json")
        mock_requests.get.assert_called_once()
        assert ret == '{"foo": "bar"}'


if __name__ == "__main__":
    unittest.main()
