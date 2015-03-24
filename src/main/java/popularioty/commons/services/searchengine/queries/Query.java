package popularioty.commons.services.searchengine.queries;

import java.util.LinkedList;
import java.util.List;

import popularioty.commons.services.searchengine.criteria.AbstractCriteria;

public  class Query 
{
	protected List<Query> subQueries;
	
	protected QueryType type;
	
	@SuppressWarnings("rawtypes")
	protected  List<AbstractCriteria> criteria;

	public Query(QueryType type) {
		this.criteria = new LinkedList<>();
		this.subQueries = new LinkedList<>();
		this.type = type;
	}
	
	public Query(List<Query> subQueries, QueryType type, List<AbstractCriteria> criteria) {
		super();
		this.subQueries = subQueries;
		this.type = type;
		this.criteria = criteria;
	}

	public List<Query> getSubQueries() {
		return subQueries;
	}

	public void setSubQueries(List<Query> subQueries) {
		this.subQueries = subQueries;
	}

	public QueryType getType() {
		return type;
	}

	public void setType(QueryType type) {
		this.type = type;
	}

	@SuppressWarnings("rawtypes")
	public List<AbstractCriteria> getCriteria() {
		return criteria;
	}

	public void setCriteria(@SuppressWarnings("rawtypes") List<AbstractCriteria> criteria) {
		this.criteria = criteria;
	}

	public void addCriteria(AbstractCriteria newValue)
	{
		this.criteria.add(newValue);
	}
	
	public void addSubQuery(Query q)
	{
		subQueries.add(q);
	}
	
}
