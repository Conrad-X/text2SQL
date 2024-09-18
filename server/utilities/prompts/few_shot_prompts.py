from utilities.utility_functions import format_schema
from utilities.prompts.base_prompt import BasePrompt
from utilities.constants.prompts_enums import FormatType, PromptType
from utilities.constants.response_messages import ERROR_NO_EXAMPLES_PROVIDED

class FullInformationOrganizationPrompt(BasePrompt):
    def get_prompt(self):
        if self.examples is None:
            raise ValueError(ERROR_NO_EXAMPLES_PROVIDED.format(prompt_type=PromptType.FULL_INFORMATION.value))
        
        formatted_schema = format_schema(FormatType.BASIC)
        prompt_lines = []

        for example in self.examples:
            prompt_lines.append(f"/* Given the following database schema : */\n{formatted_schema}\n")
            prompt_lines.append(f"/* Answer the following : {example['question']} */\n")
            prompt_lines.append(f"{example['answer']}\n")
        
        prompt_lines.append(f"/* Given the following database schema : */\n{formatted_schema}\n")
        prompt_lines.append(f"/* Answer the following : {self.target_question} */\n")
        prompt_lines.append("SELECT")
        
        return "\n".join(prompt_lines)


class SQLOnlyOrganizationPrompt(BasePrompt):
    def get_prompt(self):
        if self.examples is None:
            raise ValueError(ERROR_NO_EXAMPLES_PROVIDED.format(prompt_type=PromptType.SQL_ONLY.value))
        
        prompt_lines = []
        prompt_lines.append(f"/* Some SQL examples are provided based on similar problems : */\n")

        for example in self.examples:
            prompt_lines.append(f"\n{example['answer']}\n")
        
        prompt_lines.append(f"{self.target_question} */\n")
        
        return "\n".join(prompt_lines)

class DailSQLOrganizationPrompt(BasePrompt):
    def get_prompt(self):
        if self.examples is None:
            raise ValueError(ERROR_NO_EXAMPLES_PROVIDED.format(prompt_type=PromptType.DAIL_SQL.value))
        
        prompt_lines = []
        prompt_lines.append(f"/* Some example questions and corresponding SQL queries are provided based on similar problems : */\n")
        
        for example in self.examples:
            prompt_lines.append(f"/* Answer the following : {example['question']} */\n")
            prompt_lines.append(f"{example['answer']}\n")
        
        prompt_lines.append(f"{self.target_question} */\n")
        
        return "\n".join(prompt_lines)
