package popularioty.commons.services.searchengine.elasticsearch;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import popularioty.commons.exception.PopulariotyException;
import popularioty.commons.exception.PopulariotyException.Level;

public class ElasticSearchAdapter {

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
