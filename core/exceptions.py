class DataQualityError(Exception):
    """Raised when scraped data fails sanity checks (e.g. 50% missing titles)."""
    pass

class ProxyConfigError(Exception):
    """Raised when there are issues with the Proxy setup."""
    pass
