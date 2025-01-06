from enum import Enum

class LLM_ENUM(Enum):
    OpenAI='openai'
    Cortex = "cortex"

SAMPLE_VALUES=3
ADD_JOINS=False
ADD_SYNONYMS=True
LLM=LLM_ENUM.OpenAI.value
OPENAI_MODEL='gpt-4o-mini'
ADD_VERIFIED_QUERIES=False
VERIFIED_QUERY_NUMBER=3
SYNONYM_NUMBER=3
