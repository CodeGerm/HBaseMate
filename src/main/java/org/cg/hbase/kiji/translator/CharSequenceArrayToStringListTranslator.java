package org.cg.hbase.kiji.translator;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Array;

/**
 * A translator for array of CharSequence translator
 * @author WZ
 *
 */
public class CharSequenceArrayToStringListTranslator 
implements KijiDataTranslator<List<String>, Array<CharSequence>> {

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Array<CharSequence> convert(List<String> recommendationList) {
		List<CharSequence> recommendationArray = new ArrayList<CharSequence>();;
		for (String s : recommendationList) {
			recommendationArray.add(s);
		}
		Array<CharSequence> recomendationArray =  new Array<CharSequence>(Schema.createArray(Schema.create(Schema.Type.STRING)), recommendationArray);
		return recomendationArray;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<String> reverseConvert(Array<CharSequence> recommendationArray) {
		List<String> recommendationList = new ArrayList<String>();
		for (CharSequence ch : recommendationArray) {
			recommendationList.add(ch.toString());
		}
		
		return recommendationList;
	}
}
