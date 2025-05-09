from utilities.constants.prompts_enums import FormatType, PromptType
from utilities.constants.response_messages import ERROR_PROMPT_TYPE_NOT_FOUND
from utilities.prompts.few_shot_prompts import *
from utilities.prompts.zero_shot_prompts import *


class PromptFactory:
    @staticmethod
    def get_prompt_class(prompt_type: PromptType, target_question: str, examples = None, shots = None, schema_format = None, schema = None, evidence = None, database_name = None) -> str:
        if prompt_type == PromptType.BASIC:
            return BasicPrompt(target_question=target_question, schema=schema, evidence=evidence, database_name=database_name).get_prompt()
        elif prompt_type == PromptType.TEXT_REPRESENTATION:
            return TextRepresentationPrompt(target_question=target_question, schema=schema, evidence=evidence, database_name=database_name).get_prompt()
        elif prompt_type == PromptType.OPENAI_DEMO:
            return OpenAIDemoPrompt(target_question=target_question, schema=schema, evidence=evidence, database_name=database_name).get_prompt()
        elif prompt_type == PromptType.CODE_REPRESENTATION:
            return CodeRepresentationPrompt(target_question=target_question, schema=schema, evidence=evidence, database_name=database_name).get_prompt()
        elif prompt_type == PromptType.ALPACA_SFT:
            return AlpacaSFTPrompt(target_question=target_question, schema=schema, evidence=evidence, database_name=database_name).get_prompt()
        elif prompt_type == PromptType.FULL_INFORMATION:
            return FullInformationOrganizationPrompt(shots=shots, target_question=target_question, schema_format=schema_format, schema=schema, evidence=evidence, database_name=database_name).get_prompt()
        elif prompt_type == PromptType.SQL_ONLY:
            return SQLOnlyOrganizationPrompt(shots=shots, target_question=target_question, evidence=evidence, database_name=database_name).get_prompt()
        elif prompt_type == PromptType.DAIL_SQL:
            return DailSQLOrganizationPrompt(shots=shots, target_question=target_question, evidence=evidence, database_name=database_name).get_prompt()
        elif prompt_type == PromptType.SEMANTIC_FULL_INFORMATION:
            return SemanticAndFullInformationOrganizationPrompt(shots=shots, target_question=target_question, schema_format=schema_format, schema=schema, evidence=evidence, database_name=database_name).get_prompt()
        elif prompt_type == PromptType.ICL_XIYAN:
            return ICLXiyanPrompt(shots=shots, target_question=target_question, schema=schema, evidence=evidence, database_name=database_name).get_prompt()
        else:
            raise ValueError(ERROR_PROMPT_TYPE_NOT_FOUND.format(prompt_type=prompt_type))
