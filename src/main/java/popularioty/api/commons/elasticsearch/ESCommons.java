package popularioty.api.commons.elasticsearch;

import java.util.Map;

import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.search.SearchHit;

public class ESCommons {

	public static Map<String, Object> addId(SearchHit hit, String idField) {
		Map<String, Object> tmp;
		tmp = hit.getSource();
		tmp.put(idField, hit.getId());
		return tmp;
	}

	public static Fuzziness getFuzziness(int levenshtein) 
	{
		if(levenshtein==0)
			return Fuzziness.ZERO;
		if(levenshtein==1)
			return Fuzziness.ONE;
		if(levenshtein==2)
			return Fuzziness.TWO;
		return Fuzziness.AUTO;
	}

}
