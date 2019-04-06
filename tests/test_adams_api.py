import unittest
from app.utilities.utilities import AdamsApiPage


class AdamsApiTestCase(unittest.TestCase):
    def test_adams_api(self):
        page = AdamsApiPage(start_date='04/04/2019', end_date='04/04/2019')
        self.assertTrue(page.status_code == 200)