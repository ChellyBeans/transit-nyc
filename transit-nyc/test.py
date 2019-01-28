import unittest
import main


class NewYorkTest(unittest.TestCase):
    def test_Grand_Central(self):
        main.create_db('./GTFS')
        route_ids = main.find_passing_station('Grand Central - 42 St')
        main.close_db()
        # print(route_ids)
        self.assertEqual([], list(set(route_ids) - set(['2', '4', '5', '6', '6X', '7', '7X', 'GS'])))


if __name__ == '__main__':
    unittest.main()
