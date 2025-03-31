class AssetsMissingError(ValueError):
    def __init__(self, msg: str, uuids: [str] = None):
        super().__init__(msg)
        self.msg = msg
        self.uuids = uuids
        if uuids is None:
            self.uuids = []
