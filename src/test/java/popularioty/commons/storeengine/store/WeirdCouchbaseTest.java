package popularioty.commons.storeengine.store;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class WeirdCouchbaseTest {

	private CouchbaseCluster cluster;
	Bucket  bucket = null;
	
		
	 //@Before
	 public   void before() throws Exception {
	    this.cluster = CouchbaseCluster.create("192.168.56.105");
	    bucket = cluster.openBucket("feedback",3, TimeUnit.MINUTES);
	 } 
	 
	 //@Test
	 public void insertDocument() {
		 
			boolean done = false;
			int i =0;
			while(!done)
			{
				try{
					Thread.sleep(400);
					JsonObject ret = JsonObject.create();
					ret.put( "key", "hello");
					ret.put( "key", "world");
					JsonDocument inserted = bucket.insert(
								JsonDocument.create("test-"+UUID.randomUUID().toString().replace("-",""), ret));
					
					done = true;
				}
				 catch (InterruptedException e) {

					 e.printStackTrace();
					 
				} catch(RuntimeException e)
				{
					System.out.println("runtime exception... ");
				}
				catch(Throwable t)
				{
					System.out.println("weird stuff... ");
					t.printStackTrace();
				}
				System.out.println("count: "+(i++));
			}
	       
	     
	 }
	 
	 //@After
	 public void close()
	 {
		 try {
			cluster.disconnect();
		} catch (Exception e) {
			System.out.println("exception while closing the cluster.");
		}
	 }
}
