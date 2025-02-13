from utilities.utility_functions import format_schema
from utilities.prompts.base_prompt import BasePrompt
from utilities.constants.prompts_enums import FormatType, PromptType
from utilities.constants.response_messages import ERROR_NO_EXAMPLES_PROVIDED, ERROR_SCHEMA_FORMAT_REQUIRED
from utilities.config import DatabaseConfig

class FullInformationOrganizationPrompt(BasePrompt):
    def get_prompt(self, matches=None):
        if self.examples is None:
            raise ValueError(ERROR_NO_EXAMPLES_PROVIDED.format(prompt_type=PromptType.FULL_INFORMATION.value))
        
        if not self.schema_format:
            raise ValueError(ERROR_SCHEMA_FORMAT_REQUIRED.format(prompt_type=PromptType.FULL_INFORMATION.value))
        
        formatted_schema = format_schema(self.schema_format, DatabaseConfig.DATABASE_URL, matches)
        prompt_lines = []

        evidence_string = f"\n/*Given the following evidence: {self.evidence}*/\n" if self.evidence else ""
        for example in self.examples:
            prompt_lines.append(f"/* Given the following database schema : */\n{formatted_schema}\n")
            prompt_lines.append(f"/* Answer the following : {example['question']} */\n")
            prompt_lines.append(f"{example['answer']}\n")
        
        prompt_lines.append(f"/*Complete sqlite SQL query only and with no explanation\nGiven the following database schema : */\n{formatted_schema}\n")
        prompt_lines.append(evidence_string)
        prompt_lines.append(f"/* Answer the following : {self.target_question} */\n")
        prompt_lines.append("SELECT")
        
        return "\n".join(prompt_lines)
    
class SemanticAndFullInformationOrganizationPrompt(BasePrompt):
    def get_prompt(self, matches=None):
        if self.examples is None:
            raise ValueError(ERROR_NO_EXAMPLES_PROVIDED.format(prompt_type=PromptType.FULL_INFORMATION.value))
        
        if not self.schema_format:
            raise ValueError(ERROR_SCHEMA_FORMAT_REQUIRED.format(prompt_type=PromptType.SEMANTIC_FULL_INFORMATION.value))
        
        formatted_schema = format_schema(self.schema_format, DatabaseConfig.DATABASE_URL, matches)
        semantic_schema = format_schema(FormatType.SEMANTIC, DatabaseConfig.DATABASE_URL)

        prompt_lines = []
        
        prompt_lines.append(f"/* Given the following information about the schema : */\n{semantic_schema}\n")
        prompt_lines.append("/* Some example questions and corresponding SQL queries are provided based on similar problems : */\n")
        evidence_string = f"\n/*Given the following evidence: {self.evidence}*/\n" if self.evidence else ""
        for example in self.examples:
            prompt_lines.append(f"/* Given the following database schema : */\n{formatted_schema}\n")
 
            prompt_lines.append(f"/* Answer the following : {example['question']} */\n")
            prompt_lines.append(f"{example['answer']}\n")
        
        prompt_lines.append(f"/*Complete sqlite SQL query only and with no explanation\nGiven the following database schema : */\n{formatted_schema}\n")
        prompt_lines.append(evidence_string)
        prompt_lines.append(f"/* Answer the following : {self.target_question} */\n")
        prompt_lines.append("SELECT")
        
        return "\n".join(prompt_lines)


class SQLOnlyOrganizationPrompt(BasePrompt):
    def get_prompt(self):
        if self.examples is None:
            raise ValueError(ERROR_NO_EXAMPLES_PROVIDED.format(prompt_type=PromptType.SQL_ONLY.value))
        
        prompt_lines = []
        evidence_string = f"\n/*Given the following evidence: {self.evidence}*/\n" if self.evidence else ""
        prompt_lines.append(f"/*Complete sqlite SQL query only and with no explanation\nSome SQL examples are provided based on similar problems : */\n")

        for example in self.examples:
            prompt_lines.append(f"\n{example['answer']}\n")
        
        prompt_lines.append(evidence_string)
        prompt_lines.append(f"{self.target_question} */\n")
        
        return "\n".join(prompt_lines)

class DailSQLOrganizationPrompt(BasePrompt):
    def get_prompt(self):
        if self.examples is None:
            raise ValueError(ERROR_NO_EXAMPLES_PROVIDED.format(prompt_type=PromptType.DAIL_SQL.value))
        
        prompt_lines = []
        evidence_string = f"\n/*Given the following evidence: {self.evidence}*/\n" if self.evidence else ""
        prompt_lines.append(f"/*Complete sqlite SQL query only and with no explanation\nSome example questions and corresponding SQL queries are provided based on similar problems : */\n")
        
        for example in self.examples:
            prompt_lines.append(f"/* Answer the following : {example['question']} */\n")
            prompt_lines.append(f"{example['answer']}\n")
        
        prompt_lines.append(evidence_string)
        prompt_lines.append(f"{self.target_question} */\n")
        
        return "\n".join(prompt_lines)
