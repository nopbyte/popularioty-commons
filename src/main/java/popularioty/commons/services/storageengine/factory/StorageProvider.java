package popularioty.commons.services.storageengine.factory;

import java.util.List;
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
	
	/**
	 * 
	 * @param id of the element to be stored
	 * @param data data to be stored
	 * @param set
	 * @return
	 * @throws PopulariotyException
	 */
	public Map<String, Object> storeData(String id, Map<String, Object> data, String set) throws PopulariotyException;
	/**
	 * 
	 * @param id of the element to be stored
	 * @param data data to be stored
	 * @param set
	 * @param timoutSecs time to live for the data in the database in secs or as date when it is higher than 30 days ( 60 * 60 * 24 * 30) see 	http://developer.couchbase.com/documentation/server/4.0/developer-guide/expiry.html
	 * @return
	 * @throws PopulariotyException
	 */
	public Map<String, Object> storeData(String id, Map<String, Object> data, String set, int timoutSecs) throws PopulariotyException;
	
	public Map<String, Object> getData(String id, String set) throws PopulariotyException;
	/**
	 * 
	 * @param ids list of ids 
	 * @param set index where the documents are looked in 
	 * @param strict throw exception if there is adocument not found? 
	 * @return list of documents, which were found in the database for the ids... if !strict no exception is thrown if there is a document missing in the database... this may help when there are delays in sycnhronizations. 
	 * @throws PopulariotyException
	 */
	public List<Map<String, Object>> getData(List<String> ids, String set, boolean strict) throws PopulariotyException;
}
