from decorators.counter import count_calls
from decorators.timer import measure_time

@count_calls
@measure_time
def dummy_function():
    return sum(range(1000))

def test_count_calls_and_timer():
    result = dummy_function()
    assert isinstance(result, int)
