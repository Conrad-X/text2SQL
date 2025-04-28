from enum import Enum

class FormatType(Enum):
    BASIC = "basic"
    TEXT = "text"
    CODE = "code"
    OPENAI = "openai"
    SEMANTIC = "semantic"
    M_SCHEMA = "m_schema"

class PromptType(Enum):
    BASIC = "basic"
    TEXT_REPRESENTATION = "text_representation"
    OPENAI_DEMO = "openai_demonstration"
    CODE_REPRESENTATION = "code_representation"
    ALPACA_SFT = "alpaca_sft"
    FULL_INFORMATION = "full_information"
    SQL_ONLY = "sql_only"
    DAIL_SQL = "dail_sql"
    SEMANTIC_FULL_INFORMATION = "semantic_full_information"
    ICL_XIYAN = "icl_xiyan"
    TASL_DUMMY_SQL = "tasl_dummy_sql"

class RefinerPromptType(Enum):
    BASIC = "basic"
    XIYAN = "xiyan"
    