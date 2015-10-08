package org.cg.hbase.kiji.translator;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.avro.util.Utf8;

/**
 * A tranlator for Map of Utf8 value
 * @author WZ
 *
 */
public class MapUtf8ToStringValueTranslator
implements KijiDataTranslator<Map<String, String>, Map<String, Utf8>> {

	@Override
	public Map<String, Utf8> convert(Map<String, String> fromObject) {
		if (fromObject == null){
			return null;
		}
		Map<String, Utf8> to = new  HashMap<String, Utf8>();
		for ( Entry<String, String> e : fromObject.entrySet() ) {
			Utf8 value = new Utf8(e.getValue());
			to.put(e.getKey(), value);
		}
		return to;
	}

	@Override
	public Map<String, String> reverseConvert(Map<String, Utf8> ToObject) {
		if (ToObject == null){
			return null;
		}
		Map<String, String> to = new  HashMap<String, String>();
		for ( Entry<String, Utf8> e : ToObject.entrySet() ) {
			to.put(e.getKey().toString(), e.getValue().toString());
		}
		return to;
	}
}
