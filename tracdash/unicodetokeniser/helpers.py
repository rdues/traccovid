# -*- coding: utf-8 -*-

import os, re
import codepoints
import logging

_props_loaded = False
_grapheme_props = {}
_word_props = {}

_G_Any                   = 0
_G_CR                    = 1
_G_Control               = 1 << 1
_G_Extend                = 1 << 2
_G_L                     = 1 << 3
_G_LF                    = 1 << 4
_G_LV                    = 1 << 5
_G_LVT                   = 1 << 6
_G_Prepend               = 1 << 7
_G_Regional_Indicator    = 1 << 8
_G_SpacingMark           = 1 << 9
_G_T                     = 1 << 10
_G_V                     = 1 << 11
_G_ZWJ                   = 1 << 12

_grapheme_props_map = {
	'Any'                   : _G_Any                   ,
	'CR'                    : _G_CR                    ,
	'Control'               : _G_Control               ,
	'Extend'                : _G_Extend                ,
	'L'                     : _G_L                     ,
	'LF'                    : _G_LF                    ,
	'LV'                    : _G_LV                    ,
	'LVT'                   : _G_LVT                   ,
	'Prepend'               : _G_Prepend               ,
	'Regional_Indicator'    : _G_Regional_Indicator    ,
	'SpacingMark'           : _G_SpacingMark           ,
	'T'                     : _G_T                     ,
	'V'                     : _G_V                     ,
	'ZWJ'                   : _G_ZWJ                   ,
}

_W_Any                   = 0
_W_ALetter               = 1
_W_CR                    = 1 << 1
_W_Double_Quote          = 1 << 2
_W_Extend                = 1 << 3
_W_ExtendNumLet          = 1 << 4
_W_Format                = 1 << 5
_W_Hebrew_Letter         = 1 << 6
_W_Katakana              = 1 << 7
_W_LF                    = 1 << 8
_W_MidLetter             = 1 << 9
_W_MidNum                = 1 << 10
_W_MidNumLet             = 1 << 11
_W_Newline               = 1 << 12
_W_Numeric               = 1 << 13
_W_Regional_Indicator    = 1 << 14
_W_Single_Quote          = 1 << 15
_W_WSegSpace             = 1 << 16
_W_ZWJ                   = 1 << 17
_W_Extended_Pictographic = 1 << 18

_word_props_map = {
	'Any'                   : _W_Any                   ,
	'ALetter'               : _W_ALetter               ,
	'CR'                    : _W_CR                    ,
	'Double_Quote'          : _W_Double_Quote          ,
	'Extend'                : _W_Extend                ,
	'ExtendNumLet'          : _W_ExtendNumLet          ,
	'Format'                : _W_Format                ,
	'Hebrew_Letter'         : _W_Hebrew_Letter         ,
	'Katakana'              : _W_Katakana              ,
	'LF'                    : _W_LF                    ,
	'MidLetter'             : _W_MidLetter             ,
	'MidNum'                : _W_MidNum                ,
	'MidNumLet'             : _W_MidNumLet             ,
	'Newline'               : _W_Newline               ,
	'Numeric'               : _W_Numeric               ,
	'Regional_Indicator'    : _W_Regional_Indicator    ,
	'Single_Quote'          : _W_Single_Quote          ,
	'WSegSpace'             : _W_WSegSpace             ,
	'ZWJ'                   : _W_ZWJ                   ,
	'Extended_Pictographic' : _W_Extended_Pictographic ,
}


def get_codepoints(string):
	"""Convert a Unicode string into a list of codepoints."""
	return codepoints.from_unicode(string)


def get_unicode(cplist):
	"""Convert a codepoint list to a Unicode string."""
	return codepoints.to_unicode(cplist)

	
def is_grapheme_break(lcp, rcp):
	lprop = get_grapheme_property(lcp)
	rprop = get_grapheme_property(rcp)
	
	# only non breaks should be included here (to make sure we don't break on graphemes after testing word breaks)
	
	# line feeds
	if lprop & (_G_CR) and rprop & (_G_LF):
		return False
	
	# Hangul syllable sequences
	if lprop & (_G_L) and rprop & (_G_L | _G_V | _G_LV | _G_LVT):
		return False
	if lprop & (_G_LV | _G_V) and rprop & (_G_V | _G_T):
		return False
	if lprop & (_G_LVT | _G_T) and rprop & (_G_T):
		return False
	
	# extending characters or ZWJ
	if rprop & (_G_Extend | _G_ZWJ):
		return False
	
	# extended grapheme clusters
	if rprop & (_G_SpacingMark):
		return False
	if lprop & (_G_Prepend):
		return False
	
	# emojis and regional indicators are tested in is_word_break
	
	return True


def get_grapheme_property(cp):
	if cp in _grapheme_props:
		return _grapheme_props[cp]
	return _W_Any
	
	
def is_word_break(llcp, lcp, rcp, rrcp):
	llprop = get_word_property(llcp)
	lprop = get_word_property(lcp)
	rprop = get_word_property(rcp)
	rrprop = get_word_property(rrcp)
	
	# line feeds and new lines
	if lprop & (_W_CR) and rprop & (_W_LF):
		return False
	if lprop & (_W_Newline | _W_CR | _W_LF):
		return True
	if rprop & (_W_Newline | _W_CR | _W_LF):
		return True
	
	# emoji modified or emoji ZWJ sequences - simplified
	if llprop & (_W_Extend) and lprop & (_W_ZWJ) and rprop & (_W_Extended_Pictographic):
		return False
	if lprop & (_W_ZWJ) and rprop & (_W_Extended_Pictographic):
		return False
	
	# horizontal whitespace
	if lprop & (_W_WSegSpace) and rprop & (_W_WSegSpace):
		return False
	
	# format and extended characters - simplified
	if rprop & (_W_Extend | _W_Format | _W_ZWJ):
		return False
	
	# letters
	if lprop & (_W_ALetter | _W_Hebrew_Letter) and rprop & (_W_ALetter | _W_Hebrew_Letter):
		return False
	
	# letters and certain punctuation
	if lprop & (_W_ALetter | _W_Hebrew_Letter) and rprop & (_W_MidLetter | _W_MidNumLet | _W_Single_Quote) and rrprop & (_W_ALetter | _W_Hebrew_Letter):
		return False
	if llprop & (_W_ALetter | _W_Hebrew_Letter) and lprop & (_W_MidLetter | _W_MidNumLet | _W_Single_Quote) and rprop & (_W_ALetter | _W_Hebrew_Letter):
		return False
	if lprop & (_W_Hebrew_Letter) and rprop & (_W_Single_Quote):
		return False
	if lprop & (_W_Hebrew_Letter) and rprop & (_W_Double_Quote) and rrprop & (_W_Hebrew_Letter):
		return False
	if llprop & (_W_Hebrew_Letter) and lprop & (_W_Double_Quote) and rprop & (_W_Hebrew_Letter):
		return False
	
	# sequences of digits
	if lprop & (_W_Numeric) and rprop & (_W_Numeric):
		return False
	if lprop & (_W_ALetter | _W_Hebrew_Letter) and rprop & (_W_Numeric):
		return False
	if lprop & (_W_Numeric) and rprop & (_W_ALetter | _W_Hebrew_Letter):
		return False
	
	# sequences of digits with commas and periods
	if llprop & (_W_Numeric) and lprop & (_W_MidNum | _W_MidNumLet | _W_Single_Quote) and rprop & (_W_Numeric):
		return False
	if lprop & (_W_Numeric) and rprop & (_W_MidNum | _W_MidNumLet | _W_Single_Quote) and rrprop & (_W_Numeric):
		return False
	
	# Katakana
	if lprop & (_W_Katakana) and rprop & (_W_Katakana):
		return False
	
	# form extenders
	if lprop & (_W_ALetter | _W_Hebrew_Letter | _W_Numeric | _W_Katakana | _W_ExtendNumLet) and rprop & (_W_ExtendNumLet):
		return False
	if lprop & (_W_ExtendNumLet) and rprop & (_W_ALetter | _W_Hebrew_Letter | _W_Numeric | _W_Katakana):
		return False
	
	# regional indicator flags - simplified
	if lprop & (_W_Regional_Indicator) and rprop & (_W_Regional_Indicator):
		return False
	
	return is_grapheme_break(lcp, rcp)
	
	

def get_word_property(cp):
	if cp in _word_props:
		return _word_props[cp]
	return _W_Any
	

	
def load_properties():
	global _props_loaded
	
	if _props_loaded:
		return True
	
	try:
		load_grapheme_props()
		load_word_props()
		load_emoji_data_props()
	except Exception as e:
		print(e)
		return False
	
	logging.info('{} grapheme properties loaded'.format(len(_grapheme_props)))
	logging.info('{} word properties loaded'.format(len(_word_props)))
	
	
	_props_loaded = True
	return True


def load_grapheme_props():
	global _grapheme_props
	
	_grapheme_props = {}
	
	props_comment_pat = re.compile(r'#.*')
	props_entry_pat = re.compile(r'^\s*([0-9A-F]+)(?:\.+([0-9A-F]+))?\s*;\s*(\w+)')
	
	with open(os.path.join(os.path.dirname(__file__), 'GraphemeBreakProperty.txt'), 'r', encoding='utf-8') as f:
		for line in f:
			#print(line.strip())
			line = props_comment_pat.sub('', line).strip()
			#print('>> ' + line)
			if line:
				m = props_entry_pat.search(line)
				if m:
					#print('**** {} .. {} = {}'.format(m.group(1), m.group(2), m.group(3)))
					
					prop = 0
					if m.group(3) in _grapheme_props_map:
						prop = _grapheme_props_map[m.group(3)]
					else:
						print('!!!!!! {}'.format(line))
						continue
					
					#if m.group(3) not in prop_types:
						#prop_types[m.group(3)] = True
					
					if m.group(2):
						start = int(m.group(1), 16)
						end = int(m.group(2), 16)
						for cp in range(start, end + 1):
							if cp in _grapheme_props:
								#print('~~~~~~ {} | {} = {} | {}'.format(hex(cp), cp, prop, _grapheme_props[cp]))
								_grapheme_props[cp] = _grapheme_props[cp] | prop
							else:
								#print('++++++ {} | {} = {}'.format(hex(cp), cp, prop))
								_grapheme_props[cp] = prop
					else:
						cp = int(m.group(1), 16)
						if cp in _grapheme_props:
							#print('~~~~~~ {} | {} = {} | {}'.format(hex(cp), cp, prop, _grapheme_props[cp]))
							_grapheme_props[cp] = _grapheme_props[cp] | prop
						else:
							#print('++++++ {} | {} = {}'.format(hex(cp), cp, prop))
							_grapheme_props[cp] = prop
				else:
					print('!!!! {}'.format(line))
	
	
def load_word_props():
	global _word_props
	
	_word_props = {}
	
	props_comment_pat = re.compile(r'#.*')
	props_entry_pat = re.compile(r'^\s*([0-9A-F]+)(?:\.+([0-9A-F]+))?\s*;\s*(\w+)')
	
	with open(os.path.join(os.path.dirname(__file__), 'WordBreakProperty.txt'), 'r', encoding='utf-8') as f:
		for line in f:
			#print(line.strip())
			line = props_comment_pat.sub('', line).strip()
			#print('>> ' + line)
			if line:
				m = props_entry_pat.search(line)
				if m:
					#print('**** {} .. {} = {}'.format(m.group(1), m.group(2), m.group(3)))
					
					prop = 0
					if m.group(3) in _word_props_map:
						prop = _word_props_map[m.group(3)]
					else:
						print('!!!!!! {}'.format(line))
						continue
					
					#if m.group(3) not in prop_types:
						#prop_types[m.group(3)] = True
					
					if m.group(2):
						start = int(m.group(1), 16)
						end = int(m.group(2), 16)
						for cp in range(start, end + 1):
							if cp in _word_props:
								#print('~~~~~~ {} | {} = {} | {}'.format(hex(cp), cp, prop, _word_props[cp]))
								_word_props[cp] = _word_props[cp] | prop
							else:
								#print('++++++ {} | {} = {}'.format(hex(cp), cp, prop))
								_word_props[cp] = prop
					else:
						cp = int(m.group(1), 16)
						if cp in _word_props:
							#print('~~~~~~ {} | {} = {} | {}'.format(hex(cp), cp, prop, _word_props[cp]))
							_word_props[cp] = _word_props[cp] | prop
						else:
							#print('++++++ {} | {} = {}'.format(hex(cp), cp, prop))
							_word_props[cp] = prop
				else:
					print('!!!! {}'.format(line))

	
def load_emoji_data_props():
	global _word_props
	
	props_comment_pat = re.compile(r'#.*')
	props_entry_pat = re.compile(r'^\s*([0-9A-F]+)(?:\.+([0-9A-F]+))?\s*;\s*(\w+)')
	
	with open(os.path.join(os.path.dirname(__file__), 'emoji-data.txt'), 'r', encoding='utf-8') as f:
		for line in f:
			#print(line.strip())
			line = props_comment_pat.sub('', line).strip()
			#print('>> ' + line)
			if line:
				m = props_entry_pat.search(line)
				if m:
					#print('**** {} .. {} = {}'.format(m.group(1), m.group(2), m.group(3)))
					
					if m.group(3) == 'Extended_Pictographic':
						prop = 0
						if m.group(3) in _word_props_map:
							prop = _word_props_map[m.group(3)]
						else:
							print('!!!!!! {}'.format(line))
							continue
						
						#if m.group(3) not in prop_types:
							#prop_types[m.group(3)] = True
						
						if m.group(2):
							start = int(m.group(1), 16)
							end = int(m.group(2), 16)
							for cp in range(start, end + 1):
								if cp in _word_props:
									#print('~~~~~~ {} | {} = {} | {}'.format(hex(cp), cp, prop, _word_props[cp]))
									_word_props[cp] = _word_props[cp] | prop
								else:
									#print('++++++ {} | {} = {}'.format(hex(cp), cp, prop))
									_word_props[cp] = prop
						else:
							cp = int(m.group(1), 16)
							if cp in _word_props:
								#print('~~~~~~ {} | {} = {} | {}'.format(hex(cp), cp, prop, _word_props[cp]))
								_word_props[cp] = _word_props[cp] | prop
							else:
								#print('++++++ {} | {} = {}'.format(hex(cp), cp, prop))
								_word_props[cp] = prop
				else:
					print('!!!! {}'.format(line))

	