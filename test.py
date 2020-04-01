import unittest
from log_analyzer import get_time_perc, get_time_avg, get_time_sum, get_time_median, get_time_max


class MyTestCase(unittest.TestCase):

    def test_get_time_perc(self):
        request_times = [1.0, 2.0, 3.0]
        total_time = 10.0
        assert get_time_perc(request_times, total_time) == 60

    def test_get_time_avg(self):
        request_times = [1.0, 3.0]
        assert get_time_avg(request_times) == 2

    def test_get_time_sum(self):
        request_times = [0.1, 9.9]
        assert get_time_sum(request_times) == 10

    def test_get_time_median(self):
        request_times = [1.0, 2.0, 3.0, 4.0]
        assert get_time_median(request_times) == 2.5

    def test_get_time_max(self):
        request_times = [1.0, 2.0]
        assert get_time_max(request_times) == 2


if __name__ == '__main__':
    unittest.main()
