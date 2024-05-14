import langid
from langdetect import detect


def detect_language(text):
    lang, confidence = langid.classify(text)
    return lang, confidence


def detect_language_v2(text):
    country = detect(text)
    return country
