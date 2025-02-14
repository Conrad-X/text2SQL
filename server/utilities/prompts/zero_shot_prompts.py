from utilities.utility_functions import format_schema
from utilities.prompts.base_prompt import BasePrompt
from utilities.constants.prompts_enums import FormatType
from utilities.config import PATH_CONFIG

class BasicPrompt(BasePrompt):

    def get_prompt(self) -> str:
        formatted_schema = format_schema(FormatType.BASIC, PATH_CONFIG.sqlite_path())
        prompt = f"""{formatted_schema}\nQ: {self.target_question}\nA: SELECT"""
        return prompt
    
class TextRepresentationPrompt(BasePrompt):

    def get_prompt(self) -> str:
        formatted_schema = format_schema(FormatType.TEXT, PATH_CONFIG.sqlite_path())
        prompt = f"""Complete sqlite SQL query only and with no explanation\nGiven the following database schema :\n{formatted_schema}\nAnswer the following: {self.target_question}\nSELECT"""
        return prompt
    
class OpenAIDemoPrompt(BasePrompt):

    def get_prompt(self) -> str:
        formatted_schema = format_schema(FormatType.OPENAI, PATH_CONFIG.sqlite_path())
        prompt = f"""### Complete sqlite SQL query only and with no explanation
### SQLite SQL tables , with their properties :
#
{formatted_schema}
#
### {self.target_question}
SELECT"""
        return prompt
    
class CodeRepresentationPrompt(BasePrompt):

    def get_prompt(self) -> str:
        formatted_schema = format_schema(FormatType.CODE, PATH_CONFIG.sqlite_path())
        prompt = f"""/* Complete sqlite SQL query only and with no explanation\nGiven the following database schema : */
{formatted_schema}

/* Answer the following : {self.target_question} */
SELECT"""
        return prompt
    
class AlpacaSFTPrompt(BasePrompt):
    
    def get_prompt(self) -> str:
        formatted_schema = format_schema(FormatType.OPENAI, PATH_CONFIG.sqlite_path())
        prompt = f"""Below is an instruction that describes a task , paired with an input that provides further context . Write a response that appropriately completes the request .
### Instruction:
Write a sql to answer the question "{self.target_question}

### Input:
{formatted_schema}

### Response:
SELECT"""
        return prompt
