package popularioty.commons.services.searchengine.criteria.aggregation;

import popularioty.commons.services.searchengine.criteria.AbstractCriteria;

/**
 * This class allows to set sort parameters to aggregate the data
 * @param <T>
 */
public  class AggregationCriteria<T> extends AbstractCriteria<T>
{
	private AggregationCriteriaType type;

	protected AggregationCriteria(String field, T value, AggregationCriteriaType type) {
		super(field, value);
		this.type = type;
	}
	
	public AggregationCriteriaType getType() {
		return type;
	}
	
	public void setType(AggregationCriteriaType type) {
		this.type = type;
	}
	
}
