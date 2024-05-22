import re


def format_slug(text):
    return normalize_key(
        text,
        word_separator='-')


def normalize_key(
        key,
        word_separator=' ',
        separate_camel_case=False,
        separate_letter_digit=False,
        process_text=str.lower):
    """
    Normalize key using a variation of the method described in
    http://stackoverflow.com/a/1176023/192092

    ONETwo   one two
    OneTwo   one two
    one-two  one two
    one_two  one two
    one2     one 2
    1two     1 two
    """
    if separate_camel_case:
        key = UPPER_LOWER_PATTERN.sub(r'\1 \2', key)
        key = LOWER_UPPER_PATTERN.sub(r'\1 \2', key)
    if separate_letter_digit:
        key = LETTER_DIGIT_PATTERN.sub(r'\1 \2', key)
        key = DIGIT_LETTER_PATTERN.sub(r'\1 \2', key)
    word_separators = [r'\W_']
    if word_separator not in word_separators:
        word_separators.append(word_separator)
    word_separator_expression = '[' + ''.join(word_separators) + ']'
    word_separator_pattern = re.compile(word_separator_expression)
    key = word_separator_pattern.sub(' ', key)
    key = compact_whitespace(key)
    key = key.replace(' ', word_separator)
    return process_text(key)


def compact_whitespace(string):
    return WHITESPACE_PATTERN.sub(' ', string).strip()


UPPER_LOWER_PATTERN = re.compile(r'(.)([A-Z][a-z]+)')
LOWER_UPPER_PATTERN = re.compile(r'([a-z0-9])([A-Z])')
LETTER_DIGIT_PATTERN = re.compile(r'([A-Za-z])([0-9])')
DIGIT_LETTER_PATTERN = re.compile(r'([0-9])([A-Za-z])')
WHITESPACE_PATTERN = re.compile(r'\s+', re.MULTILINE)
