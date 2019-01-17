package com.revature.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PercentageYearWritable implements WritableComparable<PercentageYearWritable> {
	private double percentage;
	private int yearOfData;	
	
	public PercentageYearWritable() {
		percentage = 0;
		yearOfData = 0;
	}
	
	public PercentageYearWritable(double percentage, int yearOfData) {
		this.percentage = percentage;
		this.yearOfData = yearOfData;
	}
	
	public double getPercentage() {
		return percentage;
	}

	public void setPercentage(double percentage) {
		this.percentage = percentage;
	}

	public int getYearOfData() {
		return yearOfData;
	}

	public void setYearOfData(int yearOfData) {
		this.yearOfData = yearOfData;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		percentage = in.readDouble();
		yearOfData = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(percentage);
		out.writeInt(yearOfData);
	}

	public static PercentageYearWritable read(DataInput in) throws IOException {
		PercentageYearWritable writable = new PercentageYearWritable();
		writable.readFields(in);
		return writable;
	}

	@Override
	public int compareTo(PercentageYearWritable o) {
		if(this.percentage < o.percentage) {
			return -1;
		}
		else if(this.percentage > o.percentage) {
			return 1;
		}
		else {
			if(this.yearOfData < o.yearOfData) {
				return -1;
			}
			else if(this.yearOfData > o.yearOfData) {
				return 1;
			}
		}
		
		return 0;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(percentage);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + yearOfData;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PercentageYearWritable other = (PercentageYearWritable) obj;
		if (Double.doubleToLongBits(percentage) != Double
				.doubleToLongBits(other.percentage))
			return false;
		if (yearOfData != other.yearOfData)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "PercentageYearWritable [percentage=" + percentage
				+ ", yearOfData=" + yearOfData + "]";
	}
}
