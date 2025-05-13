"""
Abstract base client for interacting with Language Model services.

This module provides a common interface for various Language Model service implementations,
defining methods for executing prompts, managing batch jobs, file operations, and chat functionality.
Concrete implementations should override these methods with service-specific logic.
"""

from abc import ABC, abstractmethod
from typing import Optional

from utilities.constants.LLM_enums import ModelType


class Client(ABC):
    """
    Abstract base client for Language Model service interactions.
    
    This class defines the interface for interacting with various Language Model services,
    providing methods for prompt execution, batch processing, file management, and chat functionality.
    Concrete implementations should inherit from this class and implement the abstract methods.
    """
    
    def __init__(self, model: Optional[ModelType], temperature: Optional[float] = 0.5, max_tokens: Optional[int] = 150, client=None):
        """
        Initialize the Language Model client with configuration parameters.
        
        Args:
            model: The language model type to use for generating responses
            temperature: Controls randomness in response generation (lower is more deterministic)
            max_tokens: Maximum number of tokens to generate in response
            client: Optional pre-configured client instance
        """
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.client = client

    def execute_prompt(self, prompt: str) -> str:
        """
        Execute a single prompt and return the generated response.
        
        Args:
            prompt: The text prompt to send to the language model
            
        Returns:
            The generated text response from the language model
        """
        pass

    def upload_batch_input_file(self, file_name: str) -> str:
        """
        Upload a file for batch processing.
        
        Args:
            file_name: Path to the file to be uploaded
            
        Returns:
            A file ID or reference to the uploaded file
        """
        pass

    def create_batch_job(self, file_id: str) -> str:
        """
        Create a batch processing job for a previously uploaded file.
        
        Args:
            file_id: ID or reference of the file to process
            
        Returns:
            A batch job ID or reference
        """
        pass

    def get_all_batches(self):
        """
        Retrieve information about all batch jobs.
        
        Returns:
            A collection of batch job information
        """
        pass

    def get_all_uploaded_files(self):
        """
        Retrieve information about all uploaded files.
        
        Returns:
            A collection of file information
        """
        pass

    def download_file(self, file_id: str, file_path: str):
        """
        Download a file from the service.
        
        Args:
            file_id: ID or reference of the file to download
            file_path: Local path where the downloaded file should be saved
        """
        pass

    def excecute_chat(self, chat, prompt):
        """
        Execute a chat-based interaction with the language model.
        
        Args:
            chat: Chat context or history
            prompt: The new prompt to send within the chat context
            
        Returns:
            The generated response within the chat context
        """
        pass
