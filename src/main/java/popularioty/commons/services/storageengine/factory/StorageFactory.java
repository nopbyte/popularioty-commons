package popularioty.commons.services.storageengine.factory;



public class StorageFactory 
{
	public static StorageProvider getStorageProvider(String type)
	{
		if(type.equals(StorageProvider.CB))
			return new CouchBaseStorage();
		return null;
	}
}
