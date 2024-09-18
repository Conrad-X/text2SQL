from enum import Enum

class FormatType(Enum):
    BASIC = "basic"
    TEXT = "text"
    CODE = "code"
    OPENAI = "openai"

class PromptType(Enum):
    BASIC = "basic"
    TEXT_REPRESENTATION = "text_representation"
    OPENAI_DEMO = "openai_demonstration"
    CODE_REPRESENTATION = "code_representation"
    ALPACA_SFT = "alpaca_sft"
    FULL_INFORMATION = "full_information"
    SQL_ONLY = "sql_only"
    DAIL_SQL = "dail_sql"