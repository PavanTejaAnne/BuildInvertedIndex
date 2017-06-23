package edu.cmpe281.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import edu.cmpe281.hadoop.BasePairWC;




public class TextIntWC extends BasePairWC<Text, IntWritable> {
    public TextIntWC() {
        super(Text.class, IntWritable.class);
    }
    public TextIntWC(Text first, IntWritable second) {
        super(Text.class, IntWritable.class, first, second );
    }
}

