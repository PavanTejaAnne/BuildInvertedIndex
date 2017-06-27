package edu.cmpe281.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
// import org.apache.hadoop.io.Text;
import lombok.RequiredArgsConstructor;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
@EqualsAndHashCode
@NoArgsConstructor
public class FileIndexInfo implements WritableComparable {
     private String fileName;
     private double score;
     private ArrayList<FileTermOffsets> termOffsets;

    
    public FileIndexInfo(String fileName, double score, ArrayList<FileTermOffsets> termOffsets){
    	this.fileName = fileName;
    	this.score = score;
    	this.termOffsets = termOffsets;
    	
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        Text fnText = new Text();
        fnText.readFields(in);
        fileName = fnText.toString();
        score = in.readDouble();
        if(termOffsets == null)
            termOffsets = new ArrayList<FileTermOffsets>();
        else
            termOffsets.clear();
        int size = in.readInt();
        for(int i = 0; i < size ; i++ ) {
            FileTermOffsets tfs = new FileTermOffsets();
            tfs.readFields(in);
            termOffsets.add(tfs);
        }
    }
    

    @Override
    public void write(DataOutput out) throws IOException {
        new Text(fileName).write(out);
        out.writeDouble(score);
        out.writeInt(termOffsets.size());
        for(FileTermOffsets tfs: termOffsets)
            tfs.write(out);
    }

    public int compareTo(Object object) {
        FileIndexInfo other = (FileIndexInfo) object;
        if (this.score > other.score)
            return 1;
        else if (this.score == other.score)
            return 0;
        else return -1;
    }
    public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public ArrayList<FileTermOffsets> getTermOffsets() {
		return termOffsets;
	}

	public void setTermOffsets(ArrayList<FileTermOffsets> termOffsets) {
		this.termOffsets = termOffsets;
	}

	public String toString() {
        return "(fn=" + this.fileName + ", score=" + this.score + ", tm_ofs=" + this.termOffsets + ")";
    }
}


