package popularioty.commons.services.search;

import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
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

public class FinalReputationSearch extends ElasticSearchNode{

	private static Logger LOG = LoggerFactory.getLogger(FinalReputationSearch.class);	
	
	private String prop_index_aggregated_rep;
	private ElasticSearchNode search;

	
	public FinalReputationSearch(Map properties, SearchProvider provider) {
		search = (ElasticSearchNode) provider;
		this.prop_index_aggregated_rep= (String) properties.get("index.aggregated");
	}
	
	
	public String getFinalReputation(String entityId, String entityType) throws PopulariotyException
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
			return hit.getId();
		
		throw new PopulariotyException("No content found",null,LOG,"Reputation aggregated value not found for entity with id: "+entityId+" and type: "+entityType ,Level.DEBUG,204);
	}
}
