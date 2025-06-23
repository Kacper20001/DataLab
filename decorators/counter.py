from functools import wraps

def count_calls(func):
    """
    Dekorator zliczający liczbę wywołań funkcji.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        wrapper.calls += 1
        print(f"[Licznik] Funkcja '{func.__name__}' wywołana {wrapper.calls} raz(y)")
        return func(*args, **kwargs)
    wrapper.calls = 0
    return wrapper