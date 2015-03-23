package popularioty.commons.services.searchengine.criteria.search;

import popularioty.commons.services.searchengine.criteria.AbstractCriteria;



public class SearchCriteria<T> extends AbstractCriteria<T>
{

	private  SearchCriteriaType type;
	
	public SearchCriteria(String field, T value,SearchCriteriaType type) {
		super(field, value);
		this.type = type;
	}
	
	public SearchCriteriaType getType() {
		return type;
	}

	public void setType(SearchCriteriaType type) {
		this.type = type;
	}

}
