package popularioty.commons.services.storageengine.factory;

import java.util.Map;

import popularioty.commons.exception.PopulariotyException;

/**
 * To decouple the external apps using the commons library from specific search index providers
 * 
 *
 */
public interface StorageProvider 
{
	/**
	 *  constants used to create classes 
	 */
	public static String CB = "couch_base";
	/**
	 * Initialize the library for storage
	 * @param configuration a map containing whatever parameters are required
	 * @throws Exception
	 */
	public void init(Map<String,Object> configuration) throws Exception;
	
	/**
	 * Method to close the connection to the underlying storage server. Must be called at the end of the lifecicle of the object.
	 * @param configuration a map containing whatever parameters are required
	 * @throws Exception
	 */
	public void close(Map<String,Object> configuration)throws Exception;
	
	public Map<String, Object> storeData(String id, Map<String, Object> data, String set) throws PopulariotyException;
	
	public Map<String, Object> getData(String id, String set) throws PopulariotyException;
	
}
