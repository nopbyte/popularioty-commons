package popularioty.commons.services.searchengine.elasticsearch;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FuzzyLikeThisQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import popularioty.commons.exception.PopulariotyException;
import popularioty.commons.exception.PopulariotyException.Level;
import popularioty.commons.services.searchengine.criteria.AbstractCriteria;
import popularioty.commons.services.searchengine.criteria.aggregation.AggregationCriteria;
import popularioty.commons.services.searchengine.criteria.aggregation.AggregationCriteriaType;
import popularioty.commons.services.searchengine.criteria.search.SearchCriteria;
import popularioty.commons.services.searchengine.criteria.search.SearchCriteriaConstants;
import popularioty.commons.services.searchengine.criteria.search.SearchCriteriaType;
import popularioty.commons.services.searchengine.criteria.sort.SortCriteria;
import popularioty.commons.services.searchengine.criteria.sort.SortCriteriaConstants;
import popularioty.commons.services.searchengine.criteria.sort.SortCriteriaType;
import popularioty.commons.services.searchengine.queries.Query;
import popularioty.commons.services.searchengine.queries.QueryResponse;
import popularioty.commons.services.searchengine.queries.QueryResponse.QueryResponseType;
import popularioty.commons.services.searchengine.queries.QueryType;
import popularioty.commons.services.storageengine.factory.StorageProvider;

public abstract class ElasticSearchAdapter {

	private static Logger LOG = LoggerFactory.getLogger(ElasticSearchAdapter.class);	

	protected StorageProvider store = null;
	
	
	protected static Fuzziness getFuzziness(int levenshtein) 
	{
		if(levenshtein==0)
			return Fuzziness.ZERO;
		if(levenshtein==1)
			return Fuzziness.ONE;
		if(levenshtein==2)
			return Fuzziness.TWO;
		return Fuzziness.AUTO;
	}


	/**
	 * This method builds a ES query for a boolean query
	 * @param q Query with the propper Criteria to be met
	 * @param reqBuilder
	 * @return
	 */
	protected QueryBuilder buildSelectQuery(Query q,
			SearchRequestBuilder reqBuilder) {
		QueryBuilder qb;
		qb = QueryBuilders.boolQuery();
		for(AbstractCriteria c : q.getCriteria())
		{
			if(c instanceof SearchCriteria && 	((SearchCriteria) c).getType().equals(SearchCriteriaType.MUST_MATCH))
			{	
				((BoolQueryBuilder) qb).must(QueryBuilders.termQuery(c.getField(), c.getValue()));
			}
			else if(c instanceof SortCriteria )
			{	
				SortCriteria sc = ((SortCriteria) c);
				if(sc.getType().equals(SortCriteriaType.RANGE))
				{
					if(sc.getField().equals(SortCriteriaConstants.FIELD_FROM))
						reqBuilder.setFrom(((Integer)sc.getValue()).intValue());
					if(sc.getField().equals(SortCriteriaConstants.FIELD_SIZE))
						reqBuilder.setSize(((Integer)sc.getValue()).intValue());
				}
				else if(sc.getType().equals(SortCriteriaType.SORT))
				{
					if(sc.getValue().equals(SortCriteriaConstants.VALUE_DESC))
						reqBuilder.addSort(sc.getField(),SortOrder.DESC);
					else if(sc.getValue().equals(SortCriteriaConstants.VALUE_ASC))
						reqBuilder.addSort(sc.getField(),SortOrder.ASC);
				}
			}
			
		}
		return qb;
	}
	
	private QueryResponse buildResponse(SearchResponse scrollResp, boolean idOnly)
	{
		QueryResponse res = new QueryResponse();
		List<String> ids = new LinkedList<String>();
		List<Map<String,Object>> all = new LinkedList<Map<String,Object>>();
		for(SearchHit hit:scrollResp.getHits())
			if( idOnly )
				ids.add(hit.getId());
			else
				all.add(hit.getSource());
		
		if(!ids.isEmpty())
		{
			res.setListofStringsResult(ids);
			res.setQueryResponsetype(QueryResponseType.LIST_OF_STRINGS);
			
		}
		else if(!all.isEmpty())
		{
			res.setListofMapsResult(all);
			res.setQueryResponsetype(QueryResponseType.LIST_OF_MAPS);
		}				
		return res;
	}
	protected QueryResponse executeSelect(Query q, String index, boolean idOnly) throws PopulariotyException
	{

		QueryResponse res = new QueryResponse();
		SearchRequestBuilder reqBuilder = getClient().prepareSearch().setIndices(index);
		
		QueryBuilder qb = null;
		if(q.getType().equals(QueryType.SELECT)|| q.getType().equals(QueryType.SELECT_ID))
		{
			qb = buildSelectQuery(q, reqBuilder);
			reqBuilder.setQuery(qb);
			SearchResponse scrollResp = reqBuilder.execute().actionGet();
			if(!scrollResp.getHits().iterator().hasNext())
				throw new PopulariotyException("No content found",null,LOG,"Reputation aggregated value not found for query"+q.toString(),Level.DEBUG,204);
			res = buildResponse(scrollResp, idOnly);
			
		}
		return res;
			/*QueryBuilder qb = QueryBuilders
                .boolQuery()
                .must(QueryBuilders.termQuery("sub_reputation_type", reputationType))
                .must(QueryBuilders.termQuery("entity_id", entityId));
		
		if(entityType !=null && !entityType.equals(""))
			qb=((BoolQueryBuilder) qb).must(QueryBuilders.termQuery("entity_type", entityType));
		
		SearchResponse scrollResp = search.getClient().prepareSearch()
				.setIndices(this.prop_index_subreputation)
				.setQuery(qb)
				.setFrom(0)
				.setSize(1)
				.addSort("date",SortOrder.DESC)
				.execute().actionGet();
		
		for(SearchHit hit:scrollResp.getHits())
			return hit.getId();
		*/
		//IMPROVE LOGGING...
		
	}


	
	
	
	protected QueryResponse executeAggregation(Query q, Object aggBuilder)
	{
		
		Map<String,String> mustMatchCriteria = new HashMap<>();
		String type = "";
		QueryResponse res = new QueryResponse();
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
						res.setQueryResponsetype(QueryResponseType.SINGLE_MAP);
						res.setMapResult(termsresult);
						return res;
					}
				}
			}
		}
		else{// TODO for now only one sub aggregation is supported
		}
		return res;
	}
	
	public abstract Client getClient();

	protected QueryResponse executeFuzzyTextSearch(Query q, String index, boolean idOnly) throws PopulariotyException
	{
		List<Map<String,Object>> all = new LinkedList<Map<String,Object>>();
		QueryResponse res = new QueryResponse();
		FuzzyLikeThisQueryBuilder qb = null;
		List<String> fields = new LinkedList<>();
		String text = "";
		Fuzziness leven = Fuzziness.AUTO;
	
		for(AbstractCriteria c : q.getCriteria())
		{
			if(c instanceof SearchCriteria )
			{	
				if(((SearchCriteria) c).getType().equals(SearchCriteriaType.MUST_MATCH)&& ((SearchCriteria) c).getType().equals(SearchCriteriaConstants.FIELD_LEVENSHTEIN))
				{
					 leven = getFuzziness(((Integer)c.getValue()).intValue());
				}
				if(((SearchCriteria) c).getType().equals(SearchCriteriaType.LIKE))
				{
					fields.add(c.getField());//the name of the text fields that are compared
					text = (String) c.getValue(); //the value... they all should be the same... so we take the last...
				}
			}
			
		}
		//Done processing query formats
		if(fields.size()==1)
			 qb = QueryBuilders.fuzzyLikeThisQuery(fields.get(0));
		else if(fields.size()==2)
			qb = QueryBuilders.fuzzyLikeThisQuery(fields.get(0),fields.get(1));//I know... horrible... any solutions?
		
		qb.likeText(text).fuzziness(leven);
		SearchRequestBuilder searchReqBuilder = getClient().prepareSearch().setIndices(index).setQuery(qb);
		for(AbstractCriteria c : q.getCriteria())
		{
			if(c instanceof SortCriteria )
			{	
				SortCriteria sc = ((SortCriteria) c);
				if(sc.getType().equals(SortCriteriaType.RANGE))
				{
					if(sc.getField().equals(SortCriteriaConstants.FIELD_SIZE))
						qb.maxQueryTerms(((Integer)sc.getValue()).intValue());
				}
				else if(sc.getType().equals(SortCriteriaType.SORT))
				{
					if(sc.getValue().equals(SortCriteriaConstants.VALUE_DESC))
						searchReqBuilder.addSort(sc.getField(),SortOrder.DESC);
					else if(sc.getValue().equals(SortCriteriaConstants.VALUE_ASC))
						searchReqBuilder.addSort(sc.getField(),SortOrder.ASC);
				}
			}	
		}
		
		SearchResponse scrollResp =searchReqBuilder.execute().actionGet();
		return buildResponse(scrollResp, idOnly);
		
		/*FuzzyLikeThisQueryBuilder qb = QueryBuilders
		.fuzzyLikeThisQuery("text", "title")
		.likeText(text)
		.fuzziness(ElasticSearchNode.getFuzziness(levenshtein));
        
		if(maxQuerySize>0)
			qb.maxQueryTerms(maxQuerySize); 
		
		
		SearchResponse scrollResp = search.getClient().prepareSearch()
				.setIndices(prop_index_feedback)
				.setQuery(qb)
				.addSort("date",SortOrder.DESC)
				.execute().actionGet();
		
		for(SearchHit hit:scrollResp.getHits())
			ret.add(hit.getId());
		
		if(ret.size()==0)
			throw new PopulariotyException("No content found",null,LOG,"Fuzzy search for text : "+text+" and levehnstein: "+levenshtein+" returned nothing...",Level.DEBUG,204);*/
	}

	
}
