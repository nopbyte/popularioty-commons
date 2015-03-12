package popularioty.commons.services.searchengine.factory;

import java.util.Map;

/**
 * To decouple the external apps using the commons library from specific search index providers
 * 
 *
 */
public interface SearchProvider 
{
	public static String ES = "elastic_search";
	
	public void init(Map<String,Object> configuration) throws Exception;
	
	public void close(Map<String,Object> configuration)throws Exception;
}
