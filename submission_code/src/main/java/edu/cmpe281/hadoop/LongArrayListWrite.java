package edu.cmpe281.hadoop;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;
// import org.apache.hadoop.io.IntWritable;

@SuppressWarnings("serial")
public class LongArrayListWrite extends ArrayList<Long> implements Writable {
    public void readFields(DataInput in) throws IOException {
        clear();
        int size = in.readInt();
        for(int i = 0; i < size ; i++ ) {
            this.add(in.readLong());
        }
    }
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.size());
        for(long data: this) {
            out.writeLong(data);
        }
    }
}

