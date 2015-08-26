package popularioty.commons.services.storageengine.factory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import popularioty.commons.exception.PopulariotyException;
import popularioty.commons.exception.PopulariotyException.Level;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.deps.io.netty.handler.timeout.TimeoutException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


public class CouchBaseStorage implements StorageProvider{

	private static Logger LOG = LoggerFactory.getLogger(CouchBaseStorage.class);	
	private CouchbaseCluster cluster;
	Map<String,Bucket>  buckets = null;
	private long timeout=1;
	private TimeUnit tunit = TimeUnit.MINUTES;
	/**
	 * 
	 * @param hosts JSON array of strings including the hosts
	 * @return List of URIS pointing to all the hosts
	 * @throws PopulariotyException in case there are JSONProcessing exceptions or IOExceptions
	 */
	private List<String> parseHosts(String hosts) throws PopulariotyException
	{
		ObjectMapper mapper = new ObjectMapper();
		JsonNode data;
		try {
			data = mapper.readTree(hosts);
			List <String>hostsList = mapper.convertValue(data, List.class);
			return hostsList;
		} catch (JsonProcessingException e) {
			throw new PopulariotyException("Configuration error. Contact the Administrator",null,LOG,"JsonProcessing (Jackson) Exception while parsing array (JSON) from properties while building CouchBaseStorage in popularioty-commons"+e.getMessage() ,Level.ERROR,500);
			
		} catch (IOException e) {
			throw new PopulariotyException("Configuration error. Contact the Administrator",null,LOG,"IO Exception while parsing array (JSON) from properties while building CouchBaseStorage in popularioty-commons"+e.getMessage() ,Level.ERROR,500);
		
		}
		
	}
	/**
	 * Ensures that there is a bucket that can be used to do the operations. But only creates one bucket per set.
	 * @param set the bucket in couchbase that wants to be oppened.
	 * @return the Bucket object from CB API connected to the bucket with name equals to set.
	 */
	private  Bucket getBucket(String set)
	{
		if(buckets.containsKey(set))
			return buckets.get(set);
		Bucket bucket = cluster.openBucket(set,timeout,tunit);
		buckets.put(set, bucket);
		return bucket;
	}
	/**
	 * Translate to TimeUnit object
	 * @param unit 0,1,2 mean miliseconds, seconds, minutes respectively
	 */
	private void initTimeUnit(int unit) {
		if(unit == 0)
			tunit = TimeUnit.MILLISECONDS;
		if(unit ==1)
			tunit = TimeUnit.SECONDS;
		if(unit ==2)
			tunit = TimeUnit.MINUTES;
	}
	
	
	@Override
	public void init(Map<String, Object> configuration) throws Exception {
		
		String hosts=  (String) configuration.get("couchbase.host");
		List<String> uris = this.parseHosts(hosts);
		this.cluster = CouchbaseCluster.create(uris);
		
		this.timeout = Long.parseLong((String) configuration.get("couchbase.timeout.value"));
		int unit= Integer.parseInt((String) configuration.get("couchbase.timeout.timeunit"));
		initTimeUnit(unit);
		
		this.buckets = new HashMap<String, Bucket>();
		
	}
	

	@Override
	public void close(Map<String, Object> configuration) throws Exception {
	
		cluster.disconnect();
			
	}	
	
	public  CouchBaseStorage() {
		
		  
	}
	

	private JsonObject rootOfRecursion(Map<String, Object> data)
	{
		return (JsonObject) convertMap(data);
	}
	
	private JsonArray convertList(List o)
	{
		JsonArray array = JsonArray.create();
		Object current = null;
		for( Iterator it = ((List) o).iterator(); it.hasNext(); )
		{
			current = it.next();
			array.add(handleNode(current));
		}
		return array;
	}
	
	private JsonObject convertMap(Map<String,Object> data)
	{
			JsonObject ret = JsonObject.create();
			for(String key: data.keySet())
			{
				Object o = data.get(key);
				ret.put(key, handleNode(o));
			}
			return ret;
	}
	private Object handleNode(Object current)
	{
		if(current instanceof List)
			return convertList((List) current);
		if(current instanceof Map)
			return convertMap((Map<String, Object>) current);
		return current;
	}
	
	public Map<String, Object> storeData(String id, Map<String, Object> data, String set) throws PopulariotyException
	{
		
		try{
			//set is the bucket name
			Bucket bucket = getBucket(set);
			JsonObject d = rootOfRecursion(data);
			JsonDocument inserted = bucket.insert(
					JsonDocument.create(id, d));
		
			/*This could be removed for performance reasons, however 
			 * it helps to make sure that data really made it into the bucket?
			*/
			JsonDocument found = bucket.get(id);
			return found.content().toMap();

		}catch(BackpressureException bex)
		{
			throw new PopulariotyException("Couchbase exception. Contact the Administrator... :(",null,LOG,"From Couchbase Client. It seems The producer outpaces the SDK. Backpressure exception. "+bex.getMessage() ,Level.DEBUG,500);
		}
		catch(RequestCancelledException rex)
		{
			throw new PopulariotyException("Couchbase exception. Contact the Administrator... :(",null,LOG,"From Couchbase Client. The operation had to be cancelled while \"in flight\" on the wire. RequestCancelledException."+rex.getMessage() ,Level.DEBUG,500);
		}
		catch(TimeoutException tex)
		{
			throw new PopulariotyException("Couchbase exception. Contact the Administrator... :(",null,LOG,"From Couchbase Client. The operation takes longer than the specified timeout. TimeoutException. "+tex.getMessage() ,Level.DEBUG,500);
		}
	
	}

	@Override
	public Map<String, Object> getData(String id, String set)
			throws PopulariotyException {
		
		Bucket bucket = getBucket(set);
		JsonDocument found = bucket.get(id);
		if(found == null)
			throw new PopulariotyException("No content found",null,LOG,"value not found for couchbase document with id: "+id,Level.DEBUG,404);
		return found.content().toMap();
		
	}
	@Override
	public List<Map<String, Object>> getData(List<String> ids, String set, boolean strict)
			throws PopulariotyException {
		List<Map<String, Object>>  ret = new LinkedList<>();
		for(String id: ids)
		{
			try{
				ret.add(this.getData(id, set));
			}catch(PopulariotyException ex)
			{
				if(!strict && ex.getHTTPErrorCode()!=404)//if one doc is missing, we can live with it...
					throw ex;
			}
		}
		return ret;
	}
	

}
