import sys
file_object = open(sys.argv[1])
actor_list = file_object.readlines()
for i in xrange(len(actor_list)):
	actor_list[i] = actor_list[i].split("|")[0]

for a in xrange(len(actor_list)):
	for b in xrange(a + 1, len(actor_list)):
		# print actor_list[a], actor_list[b]
		print "hadoop-2.7.2/bin/hadoop jar PrefetchOpen.jar PrefetchOpen release/actors release/movies release/cache \"%s\" \"%s\" >>combine_results"%( actor_list[a], actor_list[b]) 
		print "echo \"a: %d, b: %d\""%(a, b)