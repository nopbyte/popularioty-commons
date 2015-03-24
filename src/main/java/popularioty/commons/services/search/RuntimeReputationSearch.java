package popularioty.commons.services.search;

import java.util.List;
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

public class RuntimeReputationSearch extends ElasticSearchNode{

	private static Logger LOG = LoggerFactory.getLogger(RuntimeReputationSearch.class);	
	
	private String prop_index_runtime_rep;
	private ElasticSearchNode search;
	private String prop_runtime_rep_prefix;

	
	public RuntimeReputationSearch(Map properties, SearchProvider provider) {
		search = (ElasticSearchNode) provider;
		this.prop_index_runtime_rep= (String) properties.get("index.runtime");
		this.prop_runtime_rep_prefix= (String) properties.get("prefix.runtime");
	}
	
	
}
