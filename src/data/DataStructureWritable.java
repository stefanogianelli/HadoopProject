package data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class DataStructureWritable implements WritableComparable<DataStructureWritable> {
	
	private Text date;
	private Text element;
	
	public DataStructureWritable () {
		date = new Text();
		element = new Text();
	}
	
	public DataStructureWritable (String date, String element) {
		this.date = new Text(date);
		this.element = new Text(element);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		date.readFields(in);
		element.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		date.write(out);
		element.write(out);
	}

	@Override
	public int compareTo(DataStructureWritable o) {
		if (o == null)
			throw new NullPointerException();
		else if (o.getDate().equals(this.getDate()) && o.getElement().equals(this.getElement()))
			return 0;
		else
			return -1;
	}	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.date == null) ? 0 : this.date.hashCode());
		result = prime * result + ((this.element == null) ? 0 : this.element.hashCode());
		return result;
	}

	@Override
	public String toString() {
		return date + "\t" + element;
	}	

	public Text getDate() {
		return date;
	}

	public void setDate(Text date) {
		this.date = date;
	}

	public Text getElement() {
		return element;
	}

	public void setElement(Text element) {
		this.element = element;
	}

}
