export const PROMPT_TYPES = {
    BASIC: "basic",
    TEXT_REPRESENTATION: "text_representation",
    OPENAI_DEMONSTRATION: "openai_demonstration",
    CODE_REPRESENTATION: "code_representation",
    ALPACA_SFT: "alpaca_sft",
    FULL_INFORMATION: "full_information",
    SQL_ONLY: "sql_only",
    DAIL_SQL: "dail_sql",
};

export const ALLOWED_PROMPT_TYPES = {
    [PROMPT_TYPES.BASIC]: 'Basic',
    [PROMPT_TYPES.TEXT_REPRESENTATION]: 'Text Representation',
    [PROMPT_TYPES.OPENAI_DEMONSTRATION]: 'OpenAI Demonstration',
    [PROMPT_TYPES.CODE_REPRESENTATION]: 'Code Representation',
    [PROMPT_TYPES.ALPACA_SFT]: 'Alpaca SFT',
    [PROMPT_TYPES.FULL_INFORMATION]: 'Full Information',
    [PROMPT_TYPES.SQL_ONLY]: 'SQL Only',
    [PROMPT_TYPES.DAIL_SQL]: 'Dail SQL'
};

export const NUMBER_OF_SHOTS_MIN = 1;
export const NUMBER_OF_SHOTS_MAX = 5;