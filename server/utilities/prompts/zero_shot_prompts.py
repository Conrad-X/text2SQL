from utilities.config import PATH_CONFIG
from utilities.constants.prompts_enums import FormatType
from utilities.prompts.base_prompt import BasePrompt
from utilities.utility_functions import format_schema


class BasicPrompt(BasePrompt):

    def get_prompt(self) -> str:
        formatted_schema = format_schema(FormatType.BASIC, self.database_name, self.schema)
        evidence_string = f"Evidence: {self.evidence}\n" if self.evidence else ""
        prompt = f"""{formatted_schema}\n{evidence_string}Q: {self.target_question}\nA: SELECT"""
        return prompt
    
class TextRepresentationPrompt(BasePrompt):

    def get_prompt(self) -> str:
        formatted_schema = format_schema(FormatType.TEXT, self.database_name, self.schema)
        evidence_string = f"Evidence: {self.evidence}\n" if self.evidence else ""
        prompt = f"""Complete sqlite SQL query only and with no explanation\nGiven the following database schema :\n{formatted_schema}\nAnswer the following: {self.target_question}\n{evidence_string}SELECT"""
        return prompt
    
class OpenAIDemoPrompt(BasePrompt):

    def get_prompt(self) -> str:
        formatted_schema = format_schema(FormatType.OPENAI, self.database_name, self.schema)
        evidence_string = f"\n#Evidence: {self.evidence}" if self.evidence else ""
        prompt = f"""### Complete sqlite SQL query only and with no explanation
### SQLite SQL tables , with their properties :
#
{formatted_schema}
### {self.target_question}
#{evidence_string}
SELECT"""
        return prompt
    
class CodeRepresentationPrompt(BasePrompt):

    def get_prompt(self) -> str:
        formatted_schema = format_schema(FormatType.CODE, self.database_name, self.schema)
        evidence_string = f"\n/* Evidence: {self.evidence}*/\n" if self.evidence else ""
        prompt = f"""/* Complete sqlite SQL query only and with no explanation\nGiven the following database schema : */
{formatted_schema}
/* Answer the following : {self.target_question} */
{evidence_string}
SELECT"""
        return prompt
    
class AlpacaSFTPrompt(BasePrompt):
    
    def get_prompt(self) -> str:
        formatted_schema = format_schema(FormatType.OPENAI, self.database_name, self.schema)
        evidence_string = f"\n### Evidence: {self.evidence}\n" if self.evidence else ""
        prompt = f"""Below is an instruction that describes a task , paired with an input that provides further context . Write a response that appropriately completes the request .
### Instruction:
Write a sql to answer the question "{self.target_question}

### Input:
{formatted_schema}
{evidence_string}
### Response:
SELECT"""
        return prompt
