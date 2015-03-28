package data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import utils.DateUtils;

public class DataStructureWritable implements
		WritableComparable<DataStructureWritable> {

	private LongWritable date;
	private Text element;

	public DataStructureWritable() {
		date = new LongWritable();
		element = new Text();
	}

	public DataStructureWritable(long date, String element) {
		this.date = new LongWritable(date);
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
	public boolean equals(Object o) {
		if (!(o instanceof DataStructureWritable))
			return false;
		DataStructureWritable other = (DataStructureWritable) o;
		if (this.getDate().equals(other.getDate())
				&& this.getElement().equals(other.getElement()))
			return true;
		else
			return false;
	}

	@Override
	public int compareTo(DataStructureWritable o) {
		if (o == null)
			throw new NullPointerException();
		else if (this.equals(o))
			return 0;
		else if (this.getDate().compareTo(o.getDate()) >= 0)
			return this.getElement().compareTo(o.getElement());
		else
			return -1;
	}

	@Override
	public int hashCode() {
		int r1 = (this.getDate() == null) ? 0 : this.getDate().hashCode();
		int r2 = (this.getElement() == null) ? 0 : this.getElement().hashCode();
		return r1 + r2;
	}

	@Override
	public String toString() {
		return DateUtils.dateToString(date.get()) + "\t" + element;
	}

	public LongWritable getDate() {
		return date;
	}

	public void setDate(LongWritable date) {
		this.date = date;
	}

	public Text getElement() {
		return element;
	}

	public void setElement(Text element) {
		this.element = element;
	}

}
