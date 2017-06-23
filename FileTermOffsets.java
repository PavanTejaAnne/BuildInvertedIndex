package edu.cmpe281.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
@EqualsAndHashCode

public class FileTermOffsets implements Writable {
  private String term;
 private LongArrayListWrite offsets;

    public FileTermOffsets(){
    	
    }
    public FileTermOffsets(String term, LongArrayListWrite offsets){
    	this.term = term;
    	this.offsets = offsets;
    }
    public String getTerm() {
		return term;
	}

	public void setTerm(String term) {
		this.term = term;
	}

	public LongArrayListWrite getOffsets() {
		return offsets;
	}

	public void setOffsets(LongArrayListWrite offsets) {
		this.offsets = offsets;
	}

	@Override
    public void readFields(DataInput in) throws IOException {
        Text tmText = new Text();
        tmText.readFields(in);
        this.term = tmText.toString();
        if (offsets == null)
            offsets = new LongArrayListWrite();
        offsets.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new Text(this.term).write(out);
        offsets.write(out);
    }

    public String toString() {
        return "(tm=" + this.term + ", ofs=" + this.offsets + ")";
    }
}


