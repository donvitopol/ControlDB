
import functools
def require_authorization(func):
    """Decorator to enforce authorization before method execution."""
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        if not getattr(self, "authorized", False):
            raise PermissionError(f"⛔ - Unauthorized access to '{func.__name__}'")
        return func(self, *args, **kwargs)
    return wrapper

