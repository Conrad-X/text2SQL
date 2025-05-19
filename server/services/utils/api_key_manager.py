"""This module provides a class for managing API keys by rotating through a list of keys."""

import random
from typing import List


class APIKeyManager:
    """
    A class to manage API keys by rotating through a list of keys.

    Attributes:
        keys (List[str]): A list of API keys.
        index (int): The current index of the API key being used.
    """

    def __init__(self, all_keys: List[str]):
        """
        Initialize the APIKeyManager with a list of API keys.

        Args:
            all_keys (List[str]): A list of API keys to manage.
        """
        self.keys = all_keys
        self.index = random.randint(0, len(all_keys) - 1)

    def get_current_key(self) -> str:
        """
        Return the current API key.

        Returns:
            str: The current API key.
        """
        return self.keys[self.index]

    def get_num_of_keys(self) -> int:
        """
        Return the number of API keys.

        Returns:
            int: The number of API keys.
        """
        return len(self.keys)

    def rotate_api_key(self):
        """
        Rotate to the next API key and return it.

        Returns:
            str: The next API key.
        """
        self.index = (self.index + 1) % self.get_num_of_keys()
        return self.get_current_key()
