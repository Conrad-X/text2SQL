import unittest

from preprocess.AddDescriptionErrorLogs import AddDescriptionErrorLogs


class TestAddDescriptionErrorLogs(unittest.TestCase):
    """Tests for the AddDescriptionErrorLogs singleton class."""

    def test_a_singleton_instance(self):
        """Test that only one instance of AddDescriptionErrorLogs is created."""
        instance1 = AddDescriptionErrorLogs()
        instance2 = AddDescriptionErrorLogs()
        self.assertIs(instance1, instance2)

    def test_b_errors_list_initialization(self):
        """Test that the errors list is initialized correctly."""
        instance = AddDescriptionErrorLogs()
        self.assertEqual(instance.errors, [])

    def test_c_add_errors(self):
        """Test that errors can be added to the errors list."""
        instance = AddDescriptionErrorLogs()
        error1 = {"message": "Error 1"}
        error2 = {"message": "Error 2"}
        instance.errors.append(error1)
        instance.errors.append(error2)
        self.assertEqual(instance.errors, [error1, error2])
