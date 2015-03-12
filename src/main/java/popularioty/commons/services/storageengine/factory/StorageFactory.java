package popularioty.commons.services.storageengine.factory;



public class StorageFactory 
{
	public static StorageProvider getSearchProvider(String type)
	{
		if(type.equals(StorageProvider.CB))
			return new CouchBaseStorage();
		return null;
	}
}
