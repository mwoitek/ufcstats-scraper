class DBNotSetupError(Exception):
    def __init__(self, message: str | None = None) -> None:
        self.message = (
            "Cannot perform this operation if the links DB is not setup" if message is None else message
        )
        super().__init__(self.message)
