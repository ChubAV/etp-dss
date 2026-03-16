class DSSError(Exception):
    pass

class FileNotFoundError(DSSError):
    pass

class TokenError(DSSError):
    pass

class AVNotPassedError(DSSError):
    pass

class FileSizeExceededError(DSSError):
    pass

class InvalidContentTypeError(DSSError):
    pass
