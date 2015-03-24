package popularioty.commons.services.searchengine.factory;

import java.util.Map;

import popularioty.commons.exception.PopulariotyException;
import popularioty.commons.services.searchengine.queries.Query;
import popularioty.commons.services.searchengine.queries.QueryResponse;


/**
 * To decouple the external apps using the commons library from specific search index providers
 * 
 *
 */
public interface SearchProvider 
{
	public static String ES = "elastic_search";
	
	public void init(Map<String,Object> configuration) throws Exception;
	
	public QueryResponse execute(Query query, String index) throws PopulariotyException;
	
	public void close(Map<String,Object> configuration)throws Exception;
}
