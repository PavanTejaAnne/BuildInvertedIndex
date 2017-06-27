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

public class TermInfo implements Writable {
   private String fileName;
    private int tf;
    private LongArrayListWrite offsets;

    public TermInfo(){
    	
    }
    public TermInfo(String fileName, int tf, LongArrayListWrite offsets){
    	this.fileName = fileName;
    	this.tf = tf;
    	this.offsets = offsets;
    }
    public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public int getTf() {
		return tf;
	}

	public void setTf(int tf) {
		this.tf = tf;
	}

	public LongArrayListWrite getOffsets() {
		return offsets;
	}

	public void setOffsets(LongArrayListWrite offsets) {
		this.offsets = offsets;
	}

	@Override
    public void readFields(DataInput in) throws IOException {
        Text fnText = new Text();
        fnText.readFields(in);
        fileName = fnText.toString();
        tf = in.readInt();
        if (offsets == null)
            offsets = new LongArrayListWrite();
        offsets.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new Text(fileName).write(out);
        out.writeInt(tf);
        offsets.write(out);
    }
    public String toString() {
        return "(fn=" + this.fileName + ", tf=" + this.tf + ", ofs=" + this.offsets + ")";
    }
}
