import datefinder
import re
from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag
from nltk.chunk import ne_chunk
from nltk.corpus import stopwords
from nltk.corpus import wordnet

# TO DO: Add Evidence/Hint from BIRD and figure out how to extract keywords if Evidence has part of SQL

def get_date_keywords(text: str):
    # Use datefinder to locate dates in the text
    matches = list(datefinder.find_dates(text, source=True))
    date_entities = []
    for _, date_str in matches:
        # date_str contains the substring that was recognized as a date
        start_index = text.find(date_str)
        if start_index != -1:
            date_entities.append({
                "entity": "DATE",
                "word": date_str,
                "start": start_index,
                "end": start_index + len(date_str)
            })

    return [entity['word'] for entity in date_entities]

def convert_word_to_singular_form(word):
    singular_word = wordnet.morphy(word)
    return singular_word if singular_word else word

def get_keywords_from_question(question: str):
    tokens = word_tokenize(question)
    pos_tags = pos_tag(tokens)

    # Perform Named Entity Recognition (NER)
    chunks = ne_chunk(pos_tags)
    named_entities = []

    # Extract named entities like 'United States', 'Acme Corp'
    for chunk in chunks:
        if hasattr(chunk, 'label') and chunk.label() in ["GPE", "PERSON", "ORGANIZATION"]:
            named_entities.append(" ".join(c[0] for c in chunk))

    # Extract all potential keywords based on POS tags (nouns and adjectives are usually key terms)
    possible_keywords = [word for word, tag in pos_tags if tag in ['NN', 'NNS', 'NNP', 'NNPS', 'JJ']]

    # Extract Date elements from the question (e.g., '8/10/2009')
    date_keywords = get_date_keywords(question)

    # Combine named entities and possible keywords (remove duplicates)
    all_keywords = set(named_entities + possible_keywords + date_keywords)

    # Dynamically filter out stopwords early
    stop_words = set(stopwords.words('english'))
    filtered_keywords = set([convert_word_to_singular_form(word) for word in all_keywords if word.lower() not in stop_words])

    return list(filtered_keywords)
