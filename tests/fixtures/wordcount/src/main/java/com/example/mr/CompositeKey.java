package com.example.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey> {
    private Text groupId = new Text();
    private LongWritable timestamp = new LongWritable();

    public CompositeKey() {}

    public CompositeKey(String groupId, long timestamp) {
        this.groupId.set(groupId);
        this.timestamp.set(timestamp);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        groupId.write(out);
        timestamp.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        groupId.readFields(in);
        timestamp.readFields(in);
    }

    @Override
    public int compareTo(CompositeKey other) {
        int cmp = this.groupId.compareTo(other.groupId);
        if (cmp != 0) return cmp;
        return this.timestamp.compareTo(other.timestamp);
    }
}
