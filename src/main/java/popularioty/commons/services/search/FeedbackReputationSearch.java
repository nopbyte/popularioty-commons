package popularioty.commons.services.search;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FuzzyLikeThisQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import popularioty.commons.exception.PopulariotyException;
import popularioty.commons.exception.PopulariotyException.Level;
import popularioty.commons.services.searchengine.factory.ElasticSearchNode;
import popularioty.commons.services.searchengine.factory.SearchProvider;

public class FeedbackReputationSearch {

	private static Logger LOG = LoggerFactory.getLogger(FeedbackReputationSearch.class);	
	
	private String prop_index_aggregated_rep;
	private String prop_index_feedback;
	private String prop_index_meta_feedback;
	private ElasticSearchNode search;
	
	public FeedbackReputationSearch(Map properties, SearchProvider provider) {
		search = (ElasticSearchNode) provider;
		this.prop_index_aggregated_rep= (String) properties.get("index.aggregated");
		this.prop_index_feedback= (String) properties.get("index.feedback");
		this.prop_index_meta_feedback= (String) properties.get("index.metafeedback");
	}
	
	
	public Map<String, Object> getFinalReputation(String entityId, String entityType) throws PopulariotyException
	{
		
		QueryBuilder qb = QueryBuilders
                .boolQuery()
                .must(QueryBuilders.termQuery("entity_id", entityId));
        
		if(entityType !=null && !entityType.equals(""))
			qb=((BoolQueryBuilder) qb).must(QueryBuilders.termQuery("entity_type", entityType));
		
		
		SearchResponse scrollResp = search.getClient().prepareSearch()
				.setIndices(this.prop_index_aggregated_rep)
				.setQuery(qb)
				.setFrom(0)
				.setSize(1)
				.addSort("date",SortOrder.DESC)
				.execute().actionGet();
		
		for(SearchHit hit:scrollResp.getHits())
			return hit.getSource();
		
		throw new PopulariotyException("No content found",null,LOG,"Reputation aggregated value not found for entity with id: "+entityId+" and type: "+entityType ,Level.DEBUG,204);
	}
	

	public List<Map<String, Object>> getFeedbackByEntity(String entityId, String entityType, int from, int size) throws PopulariotyException
	{
		List<Map<String,Object>> ret = new LinkedList<>();
		Map<String, Object> tmp = null;
		QueryBuilder qb = QueryBuilders
                .boolQuery()
                .must(QueryBuilders.termQuery("entity_type", entityType))
                .must(QueryBuilders.termQuery("entity_id", entityId));
		SearchResponse scrollResp = search.getClient().prepareSearch()
				.setIndices(prop_index_feedback)
				.setQuery(qb)
				.setFrom(from)
				.setSize(size)
				.addSort("date",SortOrder.DESC)
				.execute().actionGet();
		
		for(SearchHit hit:scrollResp.getHits())
		{
			tmp = ElasticSearchNode.addId(hit,"feedback_id");
			ret.add(tmp);
		}
		
		if(ret.size()==0)
			throw new PopulariotyException("No content found",null,LOG,"Feedback value not found for entity with id: "+entityId+" and type: "+entityType ,Level.DEBUG,204);
		return ret;
	}

	public List<Map<String, Object>> getMetaFeedbackByFeedback(String feedbackId, int from, int size) throws PopulariotyException
	{
		List<Map<String,Object>> ret = new LinkedList<>();
		Map<String, Object> tmp = null;
		QueryBuilder qb = QueryBuilders
                .boolQuery()
                .must(QueryBuilders.termQuery("feedback_id", feedbackId));
		SearchResponse scrollResp = search.getClient().prepareSearch()
				.setIndices(prop_index_meta_feedback)
				.setQuery(qb)
				.setFrom(from)
				.setSize(size)
				.addSort("date",SortOrder.DESC)
				.execute().actionGet();
		
		for(SearchHit hit:scrollResp.getHits())
		{
			tmp = ElasticSearchNode.addId(hit,"meta_feedback_id");
			ret.add(tmp);
		}
		
		if(ret.size()==0)
			throw new PopulariotyException("No content found",null,LOG,"Feedback value not found for feedback with id: "+feedbackId ,Level.DEBUG,204);
		return ret;
	}
	
	public List<Map<String, Object>> getFeedbackLevenshteinString(String text, int maxQuerySize, int levenshtein) throws PopulariotyException
	{
		if(maxQuerySize>50)
			throw new PopulariotyException("too many results requested",null,LOG,"too many results requested for fuzzy search of feedback" ,Level.DEBUG,422);
		
		List<Map<String,Object>> ret = new LinkedList<>();
		Map<String, Object> tmp = null;
		FuzzyLikeThisQueryBuilder qb = QueryBuilders
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
		{
			tmp = ElasticSearchNode.addId(hit,"feedback_id");
			ret.add(tmp);
		}
		
		if(ret.size()==0)
			throw new PopulariotyException("No content found",null,LOG,"Fuzzy search for text : "+text+" and levehnstein: "+levenshtein+" returned nothing...",Level.DEBUG,204);
		return ret;
		
	}
}
