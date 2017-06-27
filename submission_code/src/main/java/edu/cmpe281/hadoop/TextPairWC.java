package edu.cmpe281.hadoop;

import org.apache.hadoop.io.Text;

import edu.cmpe281.hadoop.BasePairWC;


public class TextPairWC extends BasePairWC<Text, Text> {
    public TextPairWC() {
        super(Text.class, Text.class);
    }
    public TextPairWC(Text first, Text second) {
        super(Text.class, Text.class, first, second );
    }
}

