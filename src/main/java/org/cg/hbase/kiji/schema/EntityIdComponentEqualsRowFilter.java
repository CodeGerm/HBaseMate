package org.cg.hbase.kiji.schema;

import com.google.common.base.Preconditions;

/**
 * A Kiji row filter that filters out rows that do not match the components specified
 * @author WZ
 *
 */
public class EntityIdComponentEqualsRowFilter {
	private final Object[] components;
	
	/**
	 * the components to match on, with nulls indicating a match on any value. 
	 * If fewer than the number of components defined in the row key format are supplied, 
	 * the rest will be filled in as null.
	 * @param components
	 */
	public EntityIdComponentEqualsRowFilter( Object... components ) {
		Preconditions.checkNotNull(components, "The components must be provided");
		this.components = components;
	}

	public Object[] getComponents() {
		return components;
	}
	
}
