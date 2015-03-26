package org.cg.hbase.kiji.translator;

/**
 * This is a data translator interface which can be used to implement
 * a translator for org.cg.hbase.kiji.schema.Translator
 * @author WZ
 *
 * @param <From>
 * @param <To>
 */
public interface KijiDataTranslator<From, To> {
	/**
	 * convert From to To
	 * @param fromObject
	 * @return
	 */
	public To convert (From fromObject);
	
	/**
	 * convert To to From
	 * @param fromObject
	 * @return
	 */
	public From reverseConvert (To ToObject);
}
