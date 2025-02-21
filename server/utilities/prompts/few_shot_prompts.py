import json

from utilities.utility_functions import format_schema
from utilities.prompts.base_prompt import BasePrompt
from utilities.constants.prompts_enums import FormatType, PromptType
from utilities.constants.response_messages import ERROR_NO_EXAMPLES_PROVIDED, ERROR_SCHEMA_FORMAT_REQUIRED
from utilities.config import PATH_CONFIG

class FullInformationOrganizationPrompt(BasePrompt):
    def get_prompt(self):
        if self.examples is None:
            raise ValueError(ERROR_NO_EXAMPLES_PROVIDED.format(prompt_type=PromptType.FULL_INFORMATION.value))
        
        if not self.schema_format:
            raise ValueError(ERROR_SCHEMA_FORMAT_REQUIRED.format(prompt_type=PromptType.FULL_INFORMATION.value))
        
        formatted_schema = format_schema(self.schema_format, PATH_CONFIG.database_name, self.schema)
        prompt_lines = []

        for example in self.examples:
            try:
                evidence_string = f"\n/* Evidence: {example['evidence']}*/"
            except:
                evidence_string = ""
            example_schema = format_schema(self.schema_format, database_name=example["db_id"], matches=json.loads(example['schema_used']), dataset_type=PATH_CONFIG.sample_dataset_type)
            prompt_lines.append(f"/* Given the following database schema : */\n{example_schema}")
            prompt_lines.append(f"/* Answer the following : {example['question']} */")
            prompt_lines.append(evidence_string)
            prompt_lines.append(f"{example['answer']}\n")
        
        evidence_string = f"/* Evidence: {self.evidence}*/" if self.evidence else ""
        prompt_lines.append(f"/*Complete sqlite SQL query only and with no explanation\nGiven the following database schema : */\n{formatted_schema}\n")
        prompt_lines.append(f"/* Answer the following : {self.target_question} */")
        prompt_lines.append(evidence_string)
        prompt_lines.append("SELECT")
        
        return "\n".join(prompt_lines)
    
class SemanticAndFullInformationOrganizationPrompt(BasePrompt):
    def get_prompt(self):
        if self.examples is None:
            raise ValueError(ERROR_NO_EXAMPLES_PROVIDED.format(prompt_type=PromptType.FULL_INFORMATION.value))
        
        if not self.schema_format:
            raise ValueError(ERROR_SCHEMA_FORMAT_REQUIRED.format(prompt_type=PromptType.SEMANTIC_FULL_INFORMATION.value))
        
        formatted_schema = format_schema(self.schema_format, PATH_CONFIG.database_name, self.schema)
        semantic_schema = format_schema(FormatType.SEMANTIC, PATH_CONFIG.database_name)

        prompt_lines = []
        
        prompt_lines.append(f"/* Given the following information about the schema : */\n{semantic_schema}\n")
        prompt_lines.append("/* Some example questions and corresponding SQL queries are provided based on similar problems : */\n")
        for example in self.examples:
            try:
                evidence_string = f"\n/*Given the following evidence: {example['evidence']}*/"
            except KeyError:
                evidence_string = ''
            example_schema = format_schema(self.schema_format, database_name=example["db_id"], matches=json.loads(example['schema_used']), dataset_type=PATH_CONFIG.sample_dataset_type)
            prompt_lines.append(f"/* Given the following database schema : */\n{example_schema}")
            prompt_lines.append(f"/* Answer the following : {example['question']} */")
            prompt_lines.append(evidence_string)
            prompt_lines.append(f"{example['answer']}\n")
        
        evidence_string = f"\n/* Evidence: {self.evidence}*/\n" if self.evidence else ""
        prompt_lines.append(f"/*Complete sqlite SQL query only and with no explanation\nGiven the following database schema : */\n{formatted_schema}\n")
        prompt_lines.append(f"/* Answer the following : {self.target_question} */\n")
        prompt_lines.append(evidence_string)
        prompt_lines.append("SELECT")
        
        return "\n".join(prompt_lines)


class SQLOnlyOrganizationPrompt(BasePrompt):
    def get_prompt(self):
        if self.examples is None:
            raise ValueError(ERROR_NO_EXAMPLES_PROVIDED.format(prompt_type=PromptType.SQL_ONLY.value))
        
        prompt_lines = []
        prompt_lines.append(f"/*Complete sqlite SQL query only and with no explanation\nSome SQL examples are provided based on similar problems : */\n")

        for example in self.examples:
            prompt_lines.append(f"\n{example['answer']}\n")
        
        evidence_string = f"\n/*Evidence: {self.evidence}*/\n" if self.evidence else ""
        prompt_lines.append(f"{self.target_question} */\n")
        prompt_lines.append(evidence_string)
        
        return "\n".join(prompt_lines)

class DailSQLOrganizationPrompt(BasePrompt):
    def get_prompt(self):
        if self.examples is None:
            raise ValueError(ERROR_NO_EXAMPLES_PROVIDED.format(prompt_type=PromptType.DAIL_SQL.value))
        
        prompt_lines = []
        prompt_lines.append(f"/*Complete sqlite SQL query only and with no explanation\nSome example questions and corresponding SQL queries are provided based on similar problems : */\n")
        
        for example in self.examples:
            try:
                evidence_string = f"/*Evidence: {example['evidence']}*/"
            except KeyError:
                evidence_string = ""
            prompt_lines.append(f"/* Answer the following : {example['question']} */")
            prompt_lines.append(f"{example['answer']}\n")
            prompt_lines.append(evidence_string)
        
        evidence_string = f"\n/*Given the following evidence: {self.evidence}*/" if self.evidence else ""
        prompt_lines.append(f"{self.target_question} */\n")
        prompt_lines.append(evidence_string)
        
        return "\n".join(prompt_lines)

class ICLXiyanPrompt(BasePrompt):
    def get_prompt(self):
        if self.examples is None:
            raise ValueError(ERROR_NO_EXAMPLES_PROVIDED.format(prompt_type=PromptType.ICL_XIYAN.value))
        
        formatted_schema = format_schema(FormatType.M_SCHEMA, PATH_CONFIG.database_name, self.schema)
        prompt_lines = []

        prompt_lines.append("You are a SQLite expert. You need to read and understand the following database schema description, as well as the evidence that may be used, and use your SQLite knowledge to generate SQL statements to answer user questions.")
        prompt_lines.append("The following examples are for your reference.")

        for example in self.examples:
            try:
                evidence_string = f"\n[Evidence]\n{example['evidence']}*/"
            except:
                evidence_string = ""
            example_schema = format_schema(FormatType.M_SCHEMA, database_name=example["db_id"], matches=json.loads(example['schema_used']), dataset_type=PATH_CONFIG.sample_dataset_type)

            prompt_lines.append(f"\n{example_schema}")
            prompt_lines.append(evidence_string)
            prompt_lines.append(f"[Question]\n{example['question']}")
            prompt_lines.append(f"```sql\n{example['answer']}\n```")
            prompt_lines.append("Question Solved.\n================")
  
        if self.evidence:
            evidence_string = f"\n[Evidence]\n{self.evidence}"
        else:
            evidence_string = ""
        prompt_lines.append(formatted_schema)
        prompt_lines.append(evidence_string)
        prompt_lines.append("[Question]")
        prompt_lines.append(self.target_question)
        prompt_lines.append("```sql")
        
        return "\n".join(prompt_lines)