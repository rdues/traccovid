# -*- coding: utf-8 -*-

import re, unicodedata


##########
# text
##########


INVALID_CHARS_REGEX = re.compile(r'[^\u0009\u000A\u000D\u0020-\uD7FF\uE000-\uFFFD\U00010000-\U0010FFFF]')

SPACES_REGEX = re.compile(r'[\u0009\u000B\u000C\u0020\u00A0\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u200B\u202F\u205F\u3000\uFEFF]')
HYPHENS_REGEX = re.compile(r'[\u002D\u00AD\u058A\u1806\u2010\u2011\u2012\u2013\u2014\u2015\u207B\u208B\u2212]')
APOSTROPHES_REGEX = re.compile(r'[\u0027\u0091\u0092\u00B4\u02BC\u02CD\u2018\u2019\u201A\u201B\u0060]')

SPACES_BETWEEN_LINES_REGEX = re.compile(r'\n[\u0009\u000B\u000C\u0020\u00A0\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u200B\u202F\u205F\u3000\uFEFF]+(?=\n)')
THREE_LINES_REGEX = re.compile(r'\n\n\n+')
MULTIPLE_SPACES_REGEX = re.compile(r'[\u0009\u000B\u000C\u0020\u00A0\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u200B\u202F\u205F\u3000\uFEFF]+')


def normalise(text):
	return unicodedata.normalize('NFC', text)


def remove_control(text):
	return INVALID_CHARS_REGEX.sub('', text)


def reduce_whitespace(text):
	text = SPACES_BETWEEN_LINES_REGEX.sub('\n', text)
	text = THREE_LINES_REGEX.sub('\n\n', text)
	text = MULTIPLE_SPACES_REGEX.sub(' ', text)
	return text.strip()


def translit_punct(text):
	text = SPACES_REGEX.sub(' ', text)
	text = HYPHENS_REGEX.sub('-', text)
	text = APOSTROPHES_REGEX.sub("'", text)
	return text



##########
# strings
##########


def contains_letter_or_digit(string):
	for c in string:
		if c.isalpha() or c.isnumeric():
			return True
	return False


def contains_digit(string):
	for c in string:
		if c.isdigit():
			return True
	return False


def index_of_one_of(string, targets):
	i = 0
	for c in string:
		if c in targets:
			return i
		i += 1
	return -1


##########
# tokens
##########

HYPHENS_LIST = ['\u002D', '\u00AD', '\u058A', '\u1806', '\u2010', '\u2011', '\u2012', '\u2013', '\u2014', '\u2015', '\u207B', '\u208B', '\u2212']
APOSTROPHES_LIST = ['\u0027', '\u0091', '\u0092', '\u00B4', '\u02BC', '\u02CD', '\u2018', '\u2019', '\u201A', '\u201B', '\u0060']


def split_apostrophes(tokens):
	new_tokens = []
	
	t = 0
	l = len(tokens)
	while t < l:
		token = tokens[t]
		while len(token) >= 3:
			idx = index_of_one_of(token, APOSTROPHES_LIST)
			if idx > 0 and idx < len(token)-1 and token[idx-1].isalpha() and token[idx+1].isalpha():
				new_tokens.append(token[:idx])
				new_tokens.append(token[idx])
				token = token[idx+1:]
			else:
				break
		if token:
			new_tokens.append(token)
		t += 1
	
	return new_tokens


def combine_hyphenated(tokens):
	new_tokens = []
	
	t = 1
	l = len(tokens) - 1
	while t < l:
		token = tokens[t]
		if len(token) == 1 and token[0] in HYPHENS_LIST and contains_letter_or_digit(tokens[t-1]) and contains_letter_or_digit(tokens[t+1]):
			new_tokens.append( tokens[t-1] + token + tokens[t+1] )
			t += 3
		else:
			new_tokens.append( tokens[t-1] )
			t += 1
	
	l = len(tokens) + 1
	while t < l:
		new_tokens.append( tokens[t-1] )
		t += 1
	
	return new_tokens


def words_only(tokens):
	new_tokens = []
	
	for token in tokens:
		if contains_letter_or_digit(token):
			new_tokens.append(token)
	
	return new_tokens


















