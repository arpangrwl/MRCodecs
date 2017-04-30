To generate WordCountSchema.java file. First create WordCountSchema.avsc file as shown below 

{
"namespace":"com.test.AvroWriteMapReduce",
"type":"record","name":"WordCountSchema",
"doc":"Avro storing of for the word count program",
"fields":[
{"name":"Word","type": ["null","string"],"default":null},
{"name":"Count","type": ["null","int"],"default":null}
]
}

Then run the below command with avro-tools-1.7.4.jar which will generate the WordCountSchema.java file.
java -jar <path/to/avro-tools-1.7.4.jar> compile schema <path/to/schema-file> <destination-folder>

Example:- 
java -jar /Users/Arpan/Documents/avro-tools-1.7.4.jar compile schema /Users/Arpan/Documents/WordCountSchema.avsc /Users/Arpan/Documents/Word_count_schema
