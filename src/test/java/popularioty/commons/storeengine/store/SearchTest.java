package popularioty.commons.storeengine.store;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import popularioty.commons.exception.PopulariotyException;
import popularioty.commons.services.search.FeedbackReputationSearch;
import popularioty.commons.services.searchengine.factory.SearchEngineFactory;
import popularioty.commons.services.searchengine.factory.SearchProvider;
import popularioty.commons.services.storageengine.factory.StorageFactory;
import popularioty.commons.services.storageengine.factory.StorageProvider;
import popularioty.commons.test.settings.Settings;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SearchTest {

	private static Settings settings;
	private static StorageProvider store; 
	private static SearchProvider search;
	private static FeedbackReputationSearch feedbackSearch;
	
	 @BeforeClass
	 public static void beforeClass() throws Exception {
	    settings = new Settings();
		store = StorageFactory.getStorageProvider(settings.getProperty("storage.engine"));
		search= SearchEngineFactory.getSearchProvider(settings.getProperty("search.engine"));
		store.init(settings.getSettings());
		search.init(settings.getSettings());
		feedbackSearch = new FeedbackReputationSearch(settings.getSettings(), search); 
	 } 
	 
	  @Test
	 public void testReadDocument() throws JsonProcessingException, IOException, PopulariotyException {
		 
		 
		 String e_id = "test_"+UUID.randomUUID().toString();
		 System.out.println("id of new entity reputation"+e_id);
		 String e_type = "test_type";
		 String doc = "{ \"user_name\":\"string\","
				 +"\"title\":\"string\","
				+"\"text\":\"string\","
				+"\"feedback_id\":\"string-abc\","
				+"\"user_id\":\"string-bcd\","
				+"\"entity_type\":\""+e_type+"\","
		        +"\"entity_id\":\""+e_id+"\","
				+"\"user_groups\": [\"multi_field\",\"index\",\"not-analyzed\"],"
		                +"\"date\": 123456789}";
		 
		    ObjectMapper mapper = new ObjectMapper();
			JsonNode data;
			String bucket = settings.getProperty("feedback.bucket");
			try{
				String id = "test-"+UUID.randomUUID().toString().replaceAll("-","");
				data = mapper.readTree(doc);
				Map insert = mapper.convertValue(data, Map.class);
				store.storeData(id, insert, bucket);
				//Give some time until the replication takes place ;)
				Thread.sleep(2000);
					
				List<String> res = feedbackSearch.getFeedbackByEntity(e_id, e_type,null, 0, 1);
				if(res==null || res.size()<=0)
					fail();
				Map result = store.getData(res.get(0), bucket);
				if(!result.equals(insert))
				{
					fail();
				}
				System.out.println("Document found:"+result.toString());
			}
			catch (PopulariotyException e) {
				fail();
				e.printStackTrace();
			}catch(RuntimeException e)
			{
				System.out.println("runtime exception... ");
				e.printStackTrace();
				fail();
			} catch (InterruptedException e)
			{
				e.printStackTrace();
				fail();
			}
			
	       
	     
	 }
	 
	 @AfterClass
	 public static void close()
	 {
		 try {
			 if(store != null)
				 store.close(null);
			 else 
				 System.err.println("store was already null while tearing down test!");
			 
			 if(search != null)
				 search.close(null);
			 else 
				 System.err.println("search was already null while tearing down test!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	 }
}
