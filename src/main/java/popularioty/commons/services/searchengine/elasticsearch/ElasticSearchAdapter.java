package popularioty.commons.services.searchengine.elasticsearch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import popularioty.commons.exception.PopulariotyException;
import popularioty.commons.services.searchengine.criteria.AbstractCriteria;
import popularioty.commons.services.searchengine.criteria.aggregation.AggregationCriteria;
import popularioty.commons.services.searchengine.criteria.aggregation.AggregationCriteriaType;
import popularioty.commons.services.searchengine.criteria.search.SearchCriteria;
import popularioty.commons.services.searchengine.criteria.search.SearchCriteriaType;
import popularioty.commons.services.searchengine.queries.Query;
import popularioty.commons.services.searchengine.queries.QueryResponse;
import popularioty.commons.services.searchengine.queries.QueryResponse.QueryResponseType;

public abstract class ElasticSearchAdapter {

	
	public QueryResponse executeAggregation(Query q, Object aggBuilder)
	{
		
		Map<String,String> mustMatchCriteria = new HashMap<>();
		String type = "";
		
		FilterAggregationBuilder built = null;
		if(aggBuilder == null)
		{
			built = AggregationBuilders.filter("agg");
			//at the moment criteria is assumed to be of the same type in each level.
			for(AbstractCriteria c : q.getCriteria())
			{
				if(c instanceof SearchCriteria && 	((SearchCriteria) c).getType().equals(SearchCriteriaType.MUST_MATCH))
				{	
					built.filter(FilterBuilders.termFilter(c.getField(), (String) c.getValue() ));
				}
			}
			if(q.getSubQueries()!=null && q.getSubQueries().size()>0)
			{
				//TODO for now only one subquery is supported..
				q = q.getSubQueries().get(0);
				for(AbstractCriteria c : q.getCriteria())
				{
					if(c instanceof AggregationCriteria && 	((AggregationCriteria) c).getType().equals(AggregationCriteriaType.TERMS))
					{
						built.subAggregation(AggregationBuilders.terms("agg_term").field(c.getField()));
						SearchResponse sr = getClient().prepareSearch().addAggregation(built).execute().actionGet();
						Filter agg = sr.getAggregations().get("agg");
						Terms term_r = agg.getAggregations().get("agg_term");
						Map<String,Object> termsresult = new HashMap<String, Object>();
						
						for (Terms.Bucket entry: term_r.getBuckets()) {
							termsresult.put(entry.getKey(),  entry.getDocCount());
						}
						QueryResponse res = new QueryResponse();
						res.setQueryResponsetype(QueryResponseType.SINGLE_MAP);
						res.setMapResult(termsresult);
						return res;
					}
				}
			}
		}
		else{// TODO for now only one sub aggregation is supported
		}
		return null;
	}
	
	public abstract Client getClient();

	public List<String> getFeedbackByEntity(String entityId, String entityType, String groupId, int from, int size) throws PopulariotyException
	{
		return null;
		/*List<String> ret = new LinkedList<>();
		Map<String, Object> tmp = null;
		QueryBuilder qb = QueryBuilders
                .boolQuery()
                .must(QueryBuilders.termQuery("entity_type", entityType))
                .must(QueryBuilders.termQuery("entity_id", entityId));
		
		if(groupId != null)
			qb=((BoolQueryBuilder) qb).must(QueryBuilders.termQuery("user_groups", groupId));
		
		SearchResponse scrollResp = search.getClient().prepareSearch()
				.setIndices(prop_index_feedback)
				.setQuery(qb)
				.setFrom(from)
				.setSize(size)
				.addSort("date",SortOrder.DESC)
				.execute().actionGet();
		
		for(SearchHit hit:scrollResp.getHits())
			ret.add(hit.getId());
		
		if(ret.size()==0)
			throw new PopulariotyException("No content found",null,LOG,"Feedback value not found for entity with id: "+entityId+" and type: "+entityType ,Level.DEBUG,204);
		return ret;*/
	}

	
}
