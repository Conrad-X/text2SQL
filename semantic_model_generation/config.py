from enum import Enum

class LLM_ENUM(Enum):
    OpenAI='openai'
    Cortex = "cortex"

SAMPLE_VALUES=0
ADD_JOINS=True
ADD_SYNONYMS=False
LLM=LLM_ENUM.Cortex.value
OPENAI_MODEL='gpt-4o-mini'
ADD_VERIFIED_QUERIES=False
VERIFIED_QUERY_NUMBER=3
SYNONYM_NUMBER=3
