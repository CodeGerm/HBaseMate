package org.cg.hbase.kiji.translator;

import org.joda.time.DateTime;

/**
 * A translator for DateTime
 * @author WZ
 *
 */
public class DateTimeToLongTranslator 
implements KijiDataTranslator<DateTime, Long> {

	@Override
	public Long convert(DateTime fromObject) {
		return fromObject.getMillis();
	}

	@Override
	public DateTime reverseConvert(Long ToObject) {
		return new DateTime(ToObject);
	}

}
