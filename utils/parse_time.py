import re

time_conversions = {
	1: ['s', 'sec', 'secs', 'second', 'seconds'],
	60: ['m', 'min', 'mins', 'minute', 'minutes'],
	60*60: ['h', 'hour', 'hours'],
	60*60*24: ['d', 'day', 'days'],
	60*60*24*7: ['w', 'week', 'weeks'],
}
time_pos_stuff_idk = [
	1,
	60,
	60*60,
	60*60*24,
]

regex_split_terms = re.compile(r"(\d+\.\d+(?!\.)|\d+)(\D+|)")
time_conversions_expanded = {}
for a in time_conversions:
    for u in time_conversions[a]:
        time_conversions_expanded[u] = a

def parse_time(text: str):
	text = text.strip()
	parse_result = regex_split_terms.findall(text)
	has_alpha = False
	parse_result.reverse()
	seconds = 0
	i = 0
	if len(parse_result) == 0:
		return -1
	for result in parse_result:
		num = float(result[0])
		unit = result[1].strip().lower()
		#print("Num: ", num, " Unit: ", unit)
		if unit in ['', '.', ';', ':']:
			if len(parse_result) == 1:
				return 60 * num
			if not has_alpha:
				if i < len(time_pos_stuff_idk):
					seconds += time_pos_stuff_idk[i] * num
				else:
					return -1
			else:
				return -1
		else:
			has_alpha = True
			if unit in time_conversions_expanded:
				seconds += time_conversions_expanded[unit] * num
			else:
				return -1
		i += 1
	return seconds
