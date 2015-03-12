package popularioty.commons.services.storageengine.factory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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


public class CouchBaseStorage implements StorageProvider{

	private static Logger LOG = LoggerFactory.getLogger(CouchBaseStorage.class);	
	private CouchbaseCluster cluster;
	private String host;
	Map<String,Bucket>  buckets = null;
	
	/**
	 * Ensures that there is a bucket that can be used to do the operations. But only creates one bucket per set.
	 * @param set the bucket in couchbase that wants to be oppened.
	 * @return the Bucket object from CB API connected to the bucket with name equals to set.
	 */
	private Bucket getBucket(String set)
	{
		if(buckets.containsKey(set))
			return buckets.get(set);
		Bucket bucket = cluster.openBucket(set);
		buckets.put(set, bucket);
		return bucket;
	}
	@Override
	public void init(Map<String, Object> configuration) throws Exception {
		
		this.host=  (String) configuration.get("couchbase.host");
		this.cluster = CouchbaseCluster.create(this.host);
		this.buckets = new HashMap<String, Bucket>();
		
	}

	@Override
	public void close(Map<String, Object> configuration) throws Exception {
	
		cluster.disconnect();
			
	}	
	
	public  CouchBaseStorage() {
		
		  
	}
	
	private JsonObject convertMap(Map<String,Object> data)
	{
		JsonObject ret = JsonObject.create();
		for(String key: data.keySet())
		{
			Object o = data.get(key);
			if(o instanceof List)
			{
				JsonArray array = JsonArray.create();
				Object current = null;
				for( Iterator it = ((List) o).iterator(); it.hasNext(); )
				{
					current = it.next();
					array.add(current);
				}
				ret.put( key , array);
			}
			else
				ret.put(key, o);
		}
		return ret;
	}
	public Map<String, Object> storeData(String id, Map<String, Object> data, String set) throws PopulariotyException
	{
		
		try{
			//set is the bucket name
			Bucket bucket = getBucket(set);
			JsonObject d = convertMap(data);
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
		return found.content().toMap();
		
	}
	

}
