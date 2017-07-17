import re
file = open("part-r-00000")
lines = file.read()
lines = re.sub('\t.*', '', lines)
with open("part.trim", "w") as f:
	f.write(lines)
