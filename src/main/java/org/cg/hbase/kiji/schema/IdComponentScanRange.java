package org.cg.hbase.kiji.schema;

/**
 * IdComponentScanRange used to scan on a specific id component range
 * @author WZ
 *
 */
public class IdComponentScanRange<T> {
	
	private final T start;
	private final T end;
	
	/**
	 * Constuctor
	 * @param start
	 * @param end
	 */
	public IdComponentScanRange(T start, T end) {
		this.start = start;
		this.end = end;
	}
	
	public T getStart() {
		return start;
	}

	public T getEnd() {
		return end;
	}
}
