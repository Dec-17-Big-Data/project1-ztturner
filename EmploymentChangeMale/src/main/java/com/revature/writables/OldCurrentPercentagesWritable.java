package com.revature.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class OldCurrentPercentagesWritable implements Writable {
	private double oldPercentage;
	private double currentPercentage;
	
	public OldCurrentPercentagesWritable() {
		oldPercentage = 0;
		currentPercentage = 0;
	}
	
	public OldCurrentPercentagesWritable(double oldPercentage, double currentPercentage) {
		super();
		this.oldPercentage = oldPercentage;
		this.currentPercentage = currentPercentage;
	}

	public double getOldPercentage() {
		return oldPercentage;
	}

	public void setOldPercentage(double oldPercentage) {
		this.oldPercentage = oldPercentage;
	}

	public double getCurrentPercentage() {
		return currentPercentage;
	}

	public void setCurrentPercentage(double currentPercentage) {
		this.currentPercentage = currentPercentage;
	}
	

	public void readFields(DataInput in) throws IOException {
		oldPercentage = in.readDouble();
		currentPercentage = in.readDouble();		
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(oldPercentage);
		out.writeDouble(currentPercentage);
	}
	
	public static OldCurrentPercentagesWritable read(DataInput in) throws IOException {
		OldCurrentPercentagesWritable writable = new OldCurrentPercentagesWritable();
		writable.readFields(in);
		return writable;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(currentPercentage);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		temp = Double.doubleToLongBits(oldPercentage);
		result = prime * result + (int) (temp ^ (temp >>> 32));
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
		OldCurrentPercentagesWritable other = (OldCurrentPercentagesWritable) obj;
		if (Double.doubleToLongBits(currentPercentage) != Double
				.doubleToLongBits(other.currentPercentage))
			return false;
		if (Double.doubleToLongBits(oldPercentage) != Double
				.doubleToLongBits(other.oldPercentage))
			return false;
		return true;
	}
	
	@Override
	public String toString() {
		return "OldCurrentPercentagesWritable [oldPercentage=" + oldPercentage
				+ ", currentPercentage=" + currentPercentage + "]";
	}
}
