MapReduce Secondary Sort Example

Requirements

IDE
Apache Maven 3.x
JVM 6 or 7
General Info

The repository contains MapReduce projects MRSecondarySort

com/stdatalabs/MRSecondarySort
PersonMapper.java -- Reads lastname and firstname and outputs (Person, firstname) as key-value pair
PersonReducer.java -- Reads the list of (Person, firstname) key-value pair and outputs sorted list of (lastname, firstname) in 2 output files
PersonPartitioner.java -- Partitions the Person composite key based on lastname
PersonSortingComparator.java -- Sorts the mapper output based on lastname and then firstname
PersonGroupingComparator.java -- Groups keys with its list of values before sending to reducer
Driver -- Driver program for MapReduce jobs




http://stdatalabs.blogspot.in/2017/02/mapreduce-vs-spark-secondary-sort.html