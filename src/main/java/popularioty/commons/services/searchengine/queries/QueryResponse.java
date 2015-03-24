package popularioty.commons.services.searchengine.queries;

import java.util.List;
import java.util.Map;

public class QueryResponse 
{
	public enum QueryResponseType{
		SINGLE_STRING,LIST_OF_STRINGS,SINGLE_MAP,LIST_OF_MAPS
	}
	private QueryResponseType queryResponsetype;
	
	private QueryResponse innerResponse;
	
	private String singleStringResult;
	
	private List<String> ListofStringsResult;
	
	private Map<String,Object> mapResult;
	
	private List<Map<String,Object>> ListofMapsResult;
	
	
	public String getSingleStringResult() {
		return singleStringResult;
	}

	public void setSingleStringResult(String singleStringResult) {
		this.singleStringResult = singleStringResult;
	}

	public List<String> getListofStringsResult() {
		return ListofStringsResult;
	}

	public void setListofStringsResult(List<String> listofStringsResult) {
		ListofStringsResult = listofStringsResult;
	}

	public Map<String, Object> getMapResult() {
		return mapResult;
	}

	public void setMapResult(Map<String, Object> mapResult) {
		this.mapResult = mapResult;
	}

	public List<Map<String, Object>> getListofMapsResult() {
		return ListofMapsResult;
	}

	public void setListofMapsResult(List<Map<String, Object>> listofMapsResult) {
		ListofMapsResult = listofMapsResult;
	}

	public QueryResponseType getQueryResponsetype() {
		return queryResponsetype;
	}

	public void setQueryResponsetype(QueryResponseType queryResponsetype) {
		this.queryResponsetype = queryResponsetype;
	}

	public QueryResponse getInnerResponse() {
		return innerResponse;
	}

	public void setInnerResponse(QueryResponse innerResponse) {
		this.innerResponse = innerResponse;
	}
	
	
	
}
