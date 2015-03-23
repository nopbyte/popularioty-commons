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
import popularioty.commons.services.searchengine.criteria.AbstractCriteria;
import popularioty.commons.services.searchengine.criteria.search.SearchCriteria;
import popularioty.commons.services.searchengine.factory.ElasticSearchNode;
import popularioty.commons.services.searchengine.factory.SearchProvider;
import popularioty.commons.services.searchengine.queries.Query;
import popularioty.commons.services.searchengine.queries.QueryType;

public class FeedbackReputationSearch {

	private static Logger LOG = LoggerFactory.getLogger(FeedbackReputationSearch.class);	
	
	private String prop_index_feedback;
	private String prop_index_meta_feedback;
	private ElasticSearchNode search;
	
	public FeedbackReputationSearch(Map properties, SearchProvider provider) {
		search = (ElasticSearchNode) provider;
		this.prop_index_feedback= (String) properties.get("index.feedback");
		this.prop_index_meta_feedback= (String) properties.get("index.metafeedback");
	}
	
	
	public List<String> getFeedbackByEntity(String entityId, String entityType, String groupId, int from, int size) throws PopulariotyException
	{
		/*Query q = new Query(QueryType.SELECT_ID); 
		if(groupId != null)
			q.addCriteria(new SearchCriteria<String>("user_groups", groupId, SearchCriteriaType.MUST_MATCH));
		q.addCriteria(new SearchCriteria<String>("entity_type", entityType, SearchCriteriaType.MUST_MATCH));
		q.addCriteria(new SearchCriteria<String>("entity_id", entityId, SearchCriteriaType.MUST_MATCH));
		
		q.addCriteria(new SortCriteria<String>("date", SortCriteriaConstants.VALUE_DESC, SortCriteriaType.SORT));
		q.addCriteria(new SortCriteria<Integer>(SortCriteriaConstants.FIELD_FROM, new Integer(from), SortCriteriaType.RANGE));
		q.addCriteria(new SortCriteria<Integer>(SortCriteriaConstants.FIELD_SIZE, new Integer(size), SortCriteriaType.RANGE));
		
		QueryResponse response = search.executeQuery(q, prop_index_feedback);
		return response.getListofStringsResult();
		*/
	
		
		List<String> ret = new LinkedList<>();
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
		return ret;
	}

	public List<String> getMetaFeedbackByFeedback(String feedbackId, int from, int size) throws PopulariotyException
	{
		List<String> ret = new LinkedList<>();
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
			ret.add(hit.getId());
		
		if(ret.size()==0)
			throw new PopulariotyException("No content found",null,LOG,"Feedback value not found for feedback with id: "+feedbackId ,Level.DEBUG,204);
		return ret;
	}
	
	public List<String> getFeedbackLevenshteinString(String text, int maxQuerySize, int levenshtein) throws PopulariotyException
	{
		if(maxQuerySize>50)
			throw new PopulariotyException("too many results requested",null,LOG,"too many results requested for fuzzy search of feedback" ,Level.DEBUG,422);
		
		List<String> ret = new LinkedList<>();
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
			ret.add(hit.getId());
		
		if(ret.size()==0)
			throw new PopulariotyException("No content found",null,LOG,"Fuzzy search for text : "+text+" and levehnstein: "+levenshtein+" returned nothing...",Level.DEBUG,204);
		return ret;
		
	}
}
