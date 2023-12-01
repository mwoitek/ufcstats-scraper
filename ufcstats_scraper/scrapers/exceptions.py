from typing import Optional

from pydantic import HttpUrl


class NoSoupError(Exception):
    def __init__(self, link: Optional[str | HttpUrl] = None) -> None:
        if link is None:
            message = "Cannot do scraping without the soup"
        else:
            message = f"Failed to get soup for {link}"
        super().__init__(message)


class MissingHTMLElementError(Exception):
    def __init__(self, description: Optional[str] = None) -> None:
        message = "Failed to find necessary HTML element(s)"
        if description is not None:
            message = f"{message}: {description}"
        super().__init__(message)
