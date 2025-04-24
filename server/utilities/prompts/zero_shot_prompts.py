from utilities.utility_functions import format_schema, get_schema_dict, get_table_foreign_keys, get_primary_keys
from utilities.prompts.base_prompt import BasePrompt
from utilities.constants.prompts_enums import FormatType
from utilities.config import PATH_CONFIG
import sqlite3
from utilities.prompts.prompt_templates import TASL_DUMMY_SQL_PROMPT
import json
from collections import defaultdict
from pprint import pprint
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

class TASLDummySQLPrompt(BasePrompt):
    
    def get_prompt(self) -> str:

        column_meanings = json.load(open(PATH_CONFIG.column_meaning_path()))

        schema_item_dict = defaultdict(lambda: defaultdict(dict))
        for key, value in column_meanings.items():
            db_id, otn, ocn = key.split('|')
            value = value.replace('#', '')
            value = value.replace('\n', ',  ')
            schema_item_dict[db_id][otn][ocn] = value

        schema_item_dict = json.loads(json.dumps(schema_item_dict))
        connection = sqlite3.connect(PATH_CONFIG.sqlite_path(database_name=self.database_name))
        schema_dict = get_schema_dict(PATH_CONFIG.sqlite_path(self.database_name))
        
        if "sqlite_sequence" in schema_dict:
            del schema_dict['sqlite_sequence']
        foreign_keys = {}
        schema_with_descriptions = {}
        for table in schema_dict:
            fks = get_table_foreign_keys(connection=connection, table_name=table)
            if len(fks)>0:
                for relation in fks:
                    key = table.lower()+"."+str(relation['from_column'])
                    value = str(relation['to_table'])+"."+str(relation['to_column'])
                    foreign_keys[key] = value
            
            schema_with_descriptions[table] = {}
            for column in schema_dict[table]:
                try:
                    schema_with_descriptions[table][column] = schema_item_dict[self.database_name][table][column]
                except KeyError:
                    schema_with_descriptions[table][column] = ""
        
        primary_keys = get_primary_keys(connection=connection)
        prompt = TASL_DUMMY_SQL_PROMPT.format(
            database_schema = json.dumps(schema_with_descriptions, indent=4),
            primary_key_dict = json.dumps(primary_keys, indent=4),
            foreign_key_dict = json.dumps(foreign_keys, indent=4),
            question_prompt = self.target_question,
            evidence = self.evidence
        )
        return prompt