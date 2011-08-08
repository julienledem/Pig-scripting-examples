#!/usr/bin/python

from org.apache.pig.scripting import *

@outputSchema("rels:{t:(id1: chararray, id2: chararray)}")
def bidirectional(id1, id2):
	if id2 != id1:
		return [ (id1, id2), (id2, id1) ]
	else:
		return [ (id1, id2) ]

def normalize(t):
	id1, id2, followed = t;
	if id2>id1:
		return (id1, id2, followed)
	else:
		return (id2, id1, followed)

@outputSchema("rels:{t:(id1: chararray, id2: chararray, followed: int)}")
def follow(to_follow_id1, links_id1, links_id2):
	outputBag = [ normalize( (links_id1, links_id2, True) ) ] 
	if to_follow_id1 is not None:
		outputBag.append( normalize( (to_follow_id1, links_id2, False) ) )
	return outputBag

@outputSchema("followed: int")
def OR(bag):
	result = False;
	for followed in bag:
		result = result | followed[0]
	return result	

@outputSchema("group: {t: (id: chararray)}")
def SORT(bag):
	bag.sort()
	return bag

def main():
	# cleanup output directory before starting
	Pig.fs("rmr out/tc")
	
	Q = Pig.compile("""
	followed = LOAD '$followed' AS (id1: chararray, id2: chararray);
	followed = FOREACH followed GENERATE FLATTEN(bidirectional(id1, id2)), 1 AS followed; -- 1 == true
	
	to_follow = LOAD '$to_follow' AS (id1: chararray, id2: chararray);
	to_follow = FOREACH to_follow GENERATE FLATTEN(bidirectional(id1, id2)), 0 AS followed; -- 0 == false
	
	links = UNION to_follow, followed;
	joined_links = JOIN links BY id1 LEFT, to_follow BY id2;
	
	new_links_dup = 
		FOREACH joined_links
		GENERATE FLATTEN( follow(to_follow::rels::id1, links::rels::id1, links::rels::id2) );

	new_links = 
		FOREACH (GROUP new_links_dup BY (id1, id2))
		GENERATE group.id1, group.id2, OR(new_links_dup.followed);

	SPLIT new_links INTO new_followed IF followed != 0, new_to_follow IF followed == 0;
	new_followed = FOREACH new_followed GENERATE id1, id2;
	new_to_follow = FOREACH new_to_follow GENERATE id1, id2;
	STORE new_followed INTO '$new_followed';
	STORE new_to_follow INTO '$new_to_follow';
	"""
	)
	
	to_follow = "data/tc_data_simple"
	followed = "out/tc/followed_0"
	
	# create empty dataset for first iteration
	Pig.fs("mkdir " + followed)
	Pig.fs("touchz " + followed + "/part-m-00000")
	
	for i in range(10):
		new_to_follow = "out/tc/to_follow_" + str(i + 1)
		new_followed = "out/tc/followed_" + str(i + 1)
		job = Q.bind().runSingle()
		if not job.isSuccessful():
			raise 'failed'
		to_follow = new_to_follow
		followed = new_followed
		# detect if we are done
		if not job.result("new_to_follow").iterator().hasNext():
			break

	Pig.compile("""
	links = LOAD '$followed' AS (id1: chararray, id2: chararray);
	links = FOREACH links GENERATE FLATTEN( bidirectional(id1, id2) );
	result = DISTINCT (
		FOREACH (GROUP links by id1)
		GENERATE SORT(links.id2));
	STORE result INTO 'out/tc/groups';
	""").bind().runSingle();

if __name__ == '__main__':
	main()