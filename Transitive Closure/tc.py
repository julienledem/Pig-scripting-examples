#!/usr/bin/python

# This Python script computes a transitive closure based on relations
# The output contains the resulting groups
#
# It requires: 
#   - an iteration: multiple calls to pig
#   - UDFs
#   - end of loop test
#   - parameter passing to Pig
#
# Notables:
#   - everything is in this script
#   - Python functions are automatically available as UDFs
#   - UDFs take the elements of the tuple as parameters, not a tuple.
#   - UDFs use standard Python constructs (Python tuple/list/dictionary instead of Pig tuple/bag/map), they are automatically converted
#   - The output schema is specified using a decorator (it can be a function if you need to manipulate the input schema)
#   - variables in the scope and python expressions can be used in the pig scripts $n, ${n-1}, ...

from org.apache.pig.scripting import *

# Pig UDFs 

@outputSchema("relationships:{t:(target:chararray, candidate:chararray)}")
def generateRelationshipsForTC(subject, object, status):
	id1, attrs1 = subject
	bag = [(id1,id1)]
	
	if object is not None:
		id2, attrs2 = object
		if status == "MATCH" and id2 != id1:
			bag.append((id1,id2))
			bag.append((id2,id1))
			bag.append((id2,id2))
			
	return bag;

@outputSchema("rels:{t:(id1:chararray, id2:chararray, status:chararray)}")
def followRel(to_join, warshall):
	outputBag = []
	for w_id1, w_id2, w_status in warshall:
		outputBag.append((w_id1, w_id2, "followed"))		
		for to_join_id1, to_join_id2, to_join_status in to_join :
			outputBag.append((to_join_id1, w_id2, to_join_status))
	return outputBag;

@outputSchema("rels:{t:(id:chararray, status:chararray)}")
def coalesceLine(bag):
	mergedGroup = {}
	for id, status in bag:
		if id not in mergedGroup or mergedGroup[id] == "notfollowed":
			mergedGroup[id] = status
	outputBag = []
	keys = mergedGroup.keys()
	keys.sort()
	for member in keys:
		outputBag.append((member,mergedGroup[member]))
	return outputBag


# main program	
def main():
	input = "data/input"
	workDir = "tmp"
	output = workDir+"/output"
	 
	# INIT
        Pig.fs("rmr "+workDir+"/warshall_0")
        Pig.fs("rmr "+workDir+"/to_join_0")
	
	# generate relationship, reverse relationship and relationship to self (only for MATCH)
	# initialize to not followed
	Pig.compile("""
		input_data = LOAD '$input' USING PigStorage AS (target:tuple(id:chararray,attributes:map[]),candidate:tuple(id:chararray,attributes:map[]),status:chararray);
		warshall_0 = FOREACH input_data GENERATE FLATTEN(generateRelationshipsForTC(*)),'notfollowed';
		to_join_0 = FILTER warshall_0 BY $0!=$1;
		STORE warshall_0 INTO '$workDir/warshall_0' USING BinStorage;
		STORE to_join_0 INTO '$workDir/to_join_0' USING BinStorage;
	""").bind().runSingle()
	
	# ----- MAIN LOOP, we don't want to iterate more than 10 times -----
	for i in range(10):
		n = i+1
		print "-------- ITER "+str(n)
        	Pig.fs("rmr "+workDir+"/warshall_"+str(n))
        	Pig.fs("rmr "+workDir+"/to_join_"+str(n))
		job = Pig.compile("""
			warshall_n_minus_1 = LOAD '$workDir/warshall_$i' USING BinStorage AS (id1:chararray, id2:chararray, status:chararray);
			to_join_n_minus_1 = LOAD '$workDir/to_join_$i' USING BinStorage AS (id1:chararray, id2:chararray, status:chararray);
			joined = COGROUP to_join_n_minus_1 BY id2, warshall_n_minus_1 BY id1;
			followed = FOREACH joined GENERATE FLATTEN(followRel(to_join_n_minus_1,warshall_n_minus_1));
			followed_byid = GROUP followed BY id1;
			warshall_n = FOREACH followed_byid GENERATE group, FLATTEN(coalesceLine(followed.(id2, status)));
			to_join_n = FILTER warshall_n BY $2 == 'notfollowed' AND $0!=$1;
			STORE warshall_n INTO '$workDir/warshall_$n' USING BinStorage;
			STORE to_join_n INTO '$workDir/to_join_$n' USING BinStorage;
		""").bind().runSingle()
		if (job.result("to_join_n").getNumberRecords() == 0) :
			break
			
	# ----- FINAL STEP -----
		
	print "-------- Computed in "+str(n)+" iterations"
		
	Pig.fs("rmr "+output)
	Pig.compile("""
		warshall_final = LOAD '$workDir/warshall_$n' USING BinStorage AS (id1:chararray, id2:chararray, status:chararray);
		materialized = GROUP warshall_final BY id1;
		members = FOREACH materialized GENERATE warshall_final.id2 as members;
		group_result = DISTINCT members;
		STORE group_result INTO '$output' USING PigStorage;
	""").bind().runSingle()
