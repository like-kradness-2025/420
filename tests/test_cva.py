from cva import _overlap_ratio

def test_overlap_ratio_partial():
    assert abs(_overlap_ratio((0, 10), (5, 15)) - 0.5) < 1e-6

