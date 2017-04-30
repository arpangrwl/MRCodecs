package com.test.AvroWriteMapReduce;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AvroWriteReducer extends Reducer<Text, IntWritable, AvroKey<WordCountSchema>, NullWritable> {
    /*reduce method accepts the Key Value pairs from mappers,
    do the aggregation based on keys and produce the final out put*/
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {
        int sum = 0;

        /*iterates through all the values available with a key and add them together and give the
        final result as the key and sum of its values*/
        for (IntWritable iw : values) {
            sum += iw.get();
        }
        // val.datum().put(key.toString(), sum);
        AvroKey<WordCountSchema> val = new AvroKey<WordCountSchema>();
        WordCountSchema wrdCntSchema = new WordCountSchema(key.toString(), sum);
        val.datum(wrdCntSchema);
        context.write(val, NullWritable.get());
    }
}