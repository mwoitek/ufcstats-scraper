from pydantic import HttpUrl


class ScraperError(Exception):
    def __init__(self, message: str | None = None) -> None:
        self.message = message
        super().__init__(self.message)


class NoSoupError(ScraperError):
    def __init__(self, link: str | HttpUrl | None = None) -> None:
        message = "Cannot do scraping without the soup" if link is None else f"Failed to get soup for {link}"
        self.message = message
        super().__init__(self.message)


class MissingHTMLElementError(ScraperError):
    def __init__(self, description: str | None = None) -> None:
        message = "Failed to find necessary HTML element(s)"
        if description is not None:
            message = f"{message}: {description}"
        self.message = message
        super().__init__(self.message)


class NoScrapedDataError(ScraperError):
    def __init__(self, link: str | HttpUrl | None = None) -> None:
        if link is None:
            message = "Cannot perform this operation with no scraped data"
        else:
            message = f"Failed to scrape data for {link}"
        self.message = message
        super().__init__(self.message)
