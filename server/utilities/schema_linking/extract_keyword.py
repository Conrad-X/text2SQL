import time
import datefinder
from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag
from nltk.chunk import ne_chunk
from nltk.corpus import stopwords
from nltk.corpus import wordnet

from utilities.logging_utils import setup_logger
from utilities.constants.script_constants import GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR
from services.base_client import Client
from utilities.prompts.prompt_templates import EXTRACT_KEYWORD_PROMPT_TEMPLATE
from utilities.constants.response_messages import UNKNOWN_ERROR

logger = setup_logger(__name__)


def get_date_keywords(text: str):
    """
    Extracts date keywords from the given text.
    """

    matches = list(datefinder.find_dates(text, source=True))
    date_entities = []
    for _, date_str in matches:
        # date_str contains the substring that was recognized as a date
        start_index = text.find(date_str)
        if start_index != -1:
            date_entities.append(
                {
                    "entity": "DATE",
                    "word": date_str,
                    "start": start_index,
                    "end": start_index + len(date_str),
                }
            )

    return [entity["word"] for entity in date_entities]


def convert_word_to_singular_form(word: str):
    """
    Converts a word to its singular form using wordnet
    """

    singular_word = wordnet.morphy(word)
    return singular_word if singular_word else word


def get_keywords_from_question(question: str, evidence: str):
    """
    Extracts keywords using NLTK from the given question and evidence.
    """

    tokens = word_tokenize(question)
    tokens = tokens + word_tokenize(evidence) if evidence else tokens

    pos_tags = pos_tag(tokens)

    # Perform Named Entity Recognition (NER)
    chunks = ne_chunk(pos_tags)
    named_entities = []

    # Extract named entities like 'United States', 'Acme Corp'
    for chunk in chunks:
        if hasattr(chunk, "label") and chunk.label() in [
            "GPE",
            "PERSON",
            "ORGANIZATION",
        ]:
            named_entities.append(" ".join(c[0] for c in chunk))

    # Extract all potential keywords based on POS tags (nouns and adjectives are usually key terms)
    possible_keywords = [
        word for word, tag in pos_tags if tag in ["NN", "NNS", "NNP", "NNPS", "JJ"]
    ]

    # Extract Date elements from the question (e.g., '8/10/2009')
    date_keywords = get_date_keywords(question)

    # Combine named entities and possible keywords (remove duplicates)
    all_keywords = set(named_entities + possible_keywords + date_keywords)

    # Dynamically filter out stopwords early
    stop_words = set(stopwords.words("english"))
    filtered_keywords = set(
        [
            convert_word_to_singular_form(word)
            for word in all_keywords
            if word.lower() not in stop_words
        ]
    )

    return list(filtered_keywords)


def get_keywords_using_LLM(
    question: str,
    evidence: str,
    client: Client
):
    """
    Extracts keywords from the given question and evidence using the specified LLM model.
    """
    
    keywords = None

    prompt = EXTRACT_KEYWORD_PROMPT_TEMPLATE.format(
        question=question, hint=evidence if evidence else ""
    )

    while keywords == None:
        try:
            keywords = client.execute_prompt(prompt=prompt)
            keywords = keywords.replace("```python", "").replace("```", "").strip()
        except Exception as e:
            if GOOGLE_RESOURCE_EXHAUSTED_EXCEPTION_STR in str(e):
                # Wait for 5 seconds before retrying
                time.sleep(5)
            else:
                logger.error(UNKNOWN_ERROR.format(e))

    return keywords
