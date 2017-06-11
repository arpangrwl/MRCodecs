package com.test.customRecordReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * Created by Arpan on 6/8/17.
 */
public class ExtendLineRecordReader extends RecordReader<Text, IntWritable> {

    private static final Log LOG = LogFactory.getLog(LineRecordReader.class);
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private Text key = null;
    private IntWritable value = null;
    private Text keyVal = null;
    private String str = null;
    private byte[] recordDelimiterBytes;

    public ExtendLineRecordReader() {
    }

    public ExtendLineRecordReader(byte[] recordDelimiter) {
        this.recordDelimiterBytes = recordDelimiter;
    }

    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
        FileSplit split = (FileSplit)genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength", 2147483647);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        Path file = split.getPath();
        this.compressionCodecs = new CompressionCodecFactory(job);
        CompressionCodec codec = this.compressionCodecs.getCodec(file);
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if(codec != null) {
            if(null == this.recordDelimiterBytes) {
                this.in = new LineReader(codec.createInputStream(fileIn), job);
            } else {
                this.in = new LineReader(codec.createInputStream(fileIn), job, this.recordDelimiterBytes);
            }

            this.end = 9223372036854775807L;
        } else {
            if(this.start != 0L) {
                skipFirstLine = true;
                --this.start;
                fileIn.seek(this.start);
            }

            if(null == this.recordDelimiterBytes) {
                this.in = new LineReader(fileIn, job);
            } else {
                this.in = new LineReader(fileIn, job, this.recordDelimiterBytes);
            }
        }

        if(skipFirstLine) {
            this.start += (long)this.in.readLine(new Text(), 0, (int)Math.min(2147483647L, this.end - this.start));
        }

        this.pos = this.start;
    }

    public boolean nextKeyValue() throws IOException {
        if(this.key == null) {
            this.key = new Text();
        }

     //   this.key.set(this.pos);
        if(this.value == null) {
            this.value = new IntWritable();
            this.keyVal = new Text();
        }

        int newSize = 0;

        while(this.pos < this.end) {
            newSize = this.in.readLine(this.keyVal, this.maxLineLength, Math.max((int)Math.min(2147483647L, this.end - this.pos), this.maxLineLength));

            if(this.keyVal!=null)
            {
                str = this.keyVal.toString();
                this.key = new Text(str.substring(0,str.indexOf('\t')));
                this.value = new IntWritable(Integer.parseInt(str.substring(str.indexOf('\t')+1)));
            }

            if(newSize == 0) {
                break;
            }

            this.pos += (long)newSize;
            if(newSize < this.maxLineLength) {
                break;
            }

            LOG.info("Skipped line of size " + newSize + " at pos " + (this.pos - (long)newSize));
        }

        if(newSize == 0) {
            this.key = null;
            this.value = null;
            return false;
        } else {
            return true;
        }
    }

    public Text getCurrentKey() {
        return this.key;
    }

    public IntWritable getCurrentValue() {
        return this.value;
    }

    public float getProgress() {
        return this.start == this.end?0.0F:Math.min(1.0F, (float)(this.pos - this.start) / (float)(this.end - this.start));
    }

    public synchronized void close() throws IOException {
        if(this.in != null) {
            this.in.close();
        }

    }

}
