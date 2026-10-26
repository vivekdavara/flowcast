"""percentile() computes ceil(p / 100 * n) in floats: p=99.9, n=1000 gives rank 1000 (the max) instead of 999,
because 99.9/100*1000 == 999.0000000000001. (p50/p95/p99 as used are exact for n <= 200,000.)"""

from flowcast.latency import percentile


def test_p999_of_1000_values():
    assert percentile([float(n) for n in range(1, 1001)], 99.9) == 999
