package popularioty.commons.services.searchengine.queries;

import java.util.List;
import java.util.Map;

public class QueryResponse 
{
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
	
	
	
}
