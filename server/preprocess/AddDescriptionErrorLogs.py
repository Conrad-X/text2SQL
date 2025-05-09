class AddDescriptionErrorLogs:
    """
    A singleton class that maintains a shared list of errors to fix.

    This class ensures that only one instance exists throughout the application.
    It uses the singleton pattern by overriding the __new__ method.

    Attributes:
        errors (list): A list to store error messages or error objects.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.errors = []  # Initialize the list
        return cls._instance