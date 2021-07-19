# -*- coding: utf-8 -*-

from . import helpers

def tokenise(text):
	"""Tokenise the text. Based on the TR29 Unicode report. Returns a list of strings."""
	if text == None or len(text) == 0:
		return []
	
	# check properties are loaded
	helpers.load_properties()
	
	tokens = []
	
	# codepoints and length
	cp = helpers.get_codepoints(text)
	l = len(cp)
	
	# position in cp list
	start = 0
	end = 0
	i = 0
	
	# codepoints around potential break point
	llcp = 0
	lcp = 0
	rcp = 0
	rrcp = 0
	
	# preload right side
	if i < l:
		rrcp = cp[i]
		i += 1
	
	if i < l:
		rcp = rrcp
		rrcp = cp[i]
		i += 1
	
	# from now on we have a left side
	# test for breaks between left and right codepoints and create tokens where found
	while i < l:
		llcp = lcp
		lcp = rcp
		rcp = rrcp
		rrcp = cp[i]
		
		if helpers.is_word_break(llcp, lcp, rcp, rrcp):
			end = i - 1
			tokens.append(cp[start:end])
			start = end
		
		# step forward one codepoint
		i += 1
	
	# still have two right side codepoints to test
	if rcp != 0 and rrcp != 0:
		llcp = lcp
		lcp = rcp
		rcp = rrcp
		rrcp = 0
		
		if helpers.is_word_break(llcp, lcp, rcp, rrcp):
			end = i - 1
			tokens.append(cp[start:end])
			start = end
		
	# whatever remains must be a token
	tokens.append(cp[start:l])
	
	# convert tokens from codepoints to unicode
	for t in range(0, len(tokens)):
		tokens[t] = helpers.get_unicode(tokens[t])
	
	return tokens


	