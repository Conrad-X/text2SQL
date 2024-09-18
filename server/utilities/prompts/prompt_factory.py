from utilities.prompts.zero_shot_prompts import *
from utilities.prompts.few_shot_prompts import *
from utilities.constants.prompts_enums import PromptType

from utilities.constants.response_messages import ERROR_PROMPT_TYPE_NOT_FOUND


class PromptFactory:
    @staticmethod
    def get_prompt_class(prompt_type: PromptType, target_question: str, examples=None, shots=None) -> str:
        if prompt_type == PromptType.BASIC:
            return BasicPrompt(target_question=target_question).get_prompt()
        elif prompt_type == PromptType.TEXT_REPRESENTATION:
            return TextRepresentationPrompt(target_question=target_question).get_prompt()
        elif prompt_type == PromptType.OPENAI_DEMO:
            return OpenAIDemoPrompt(target_question=target_question).get_prompt()
        elif prompt_type == PromptType.CODE_REPRESENTATION:
            return CodeRepresentationPrompt(target_question=target_question).get_prompt()
        elif prompt_type == PromptType.ALPACA_SFT:
            return AlpacaSFTPrompt(target_question=target_question).get_prompt()
        elif prompt_type == PromptType.FULL_INFORMATION:
            return FullInformationOrganizationPrompt(shots=shots, target_question=target_question).get_prompt()
        elif prompt_type == PromptType.SQL_ONLY:
            return SQLOnlyOrganizationPrompt(shots=shots, target_question=target_question).get_prompt()
        elif prompt_type == PromptType.DIAL_SQL:
            return DialSQLOrganizationPrompt(shots=shots, target_question=target_question).get_prompt()
        else:
            raise ValueError(ERROR_PROMPT_TYPE_NOT_FOUND.format(prompt_type=prompt_type))
