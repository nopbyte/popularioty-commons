package popularioty.commons.services.searchengine.factory;


public class SearchEngineFactory 
{
	public static SearchProvider getSearchProvider(String type)
	{
		if(type.equals(SearchProvider.ES))
			return new ElasticSearchNode();
		return null;
	}
}
