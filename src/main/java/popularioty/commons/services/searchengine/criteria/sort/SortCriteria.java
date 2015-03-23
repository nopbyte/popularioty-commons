package popularioty.commons.services.searchengine.criteria.sort;

import popularioty.commons.services.searchengine.criteria.AbstractCriteria;

/**
 * This class allows to set sort parameters such as from, to, and sort
 * @param <T>
 */
public  class SortCriteria<T> extends AbstractCriteria<T>
{
	private SortCriteriaType type;
	
	public SortCriteria(String field, T value, SortCriteriaType type) {
		super(field, value);
		this.type = type;
	}		
	
	public SortCriteriaType getType() {
		return type;
	}
	
	public void setType(SortCriteriaType type) {
		this.type = type;
	}

	
}
