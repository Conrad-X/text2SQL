from enum import Enum

class FormatType(Enum):
    BASIC = "basic"
    TEXT = "text"
    CODE = "code"
    OPENAI = "openai"

class PromptType(Enum):
    BASIC = "basic"
    TEXT_REPRESENTATION = "text"
    OPENAI_DEMO = "openai"
    CODE_REPRESENTATION = "code"
    ALPACA_SFT = "alpaca_sft"
    FULL_INFORMATION = "full_information"
    SQL_ONLY = "sql_only"
    DIAL_SQL = "dial_sql"