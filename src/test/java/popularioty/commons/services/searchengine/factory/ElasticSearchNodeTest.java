package popularioty.commons.services.searchengine.factory;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import popularioty.commons.exception.PopulariotyException;
import popularioty.commons.services.searchengine.criteria.aggregation.AggregationCriteria;
import popularioty.commons.services.searchengine.criteria.aggregation.AggregationCriteriaType;
import popularioty.commons.services.searchengine.criteria.search.SearchCriteria;
import popularioty.commons.services.searchengine.criteria.search.SearchCriteriaType;
import popularioty.commons.services.searchengine.factory.SearchEngineFactory;
import popularioty.commons.services.searchengine.factory.SearchProvider;
import popularioty.commons.services.searchengine.queries.Query;
import popularioty.commons.services.searchengine.queries.QueryResponse;
import popularioty.commons.services.searchengine.queries.QueryType;
import popularioty.commons.services.storageengine.factory.StorageFactory;
import popularioty.commons.services.storageengine.factory.StorageProvider;
import popularioty.commons.test.settings.Settings;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ElasticSearchNodeTest {

	private static Settings settings;
	private static ElasticSearchNode search;
	
	
	 //@BeforeClass
	 public static void beforeClass() throws Exception {
	    settings = new Settings();
		search= new ElasticSearchNode();
		search.init(settings.getSettings());	 
	 } 
	 
	 //@Test
	 public void testAggregation() throws JsonProcessingException, IOException, PopulariotyException {
		 
		  Map<String,String> map = new HashMap<String, String>();
		  map.put("couchbaseDocument.doc.src.soid",  "1426859511066689111ae7e5e46dca7066ec2bc59b3b5");
		  Map<String,Long> res=search.getCountOfDocumentsByTerm(map, "couchbaseDocument.doc.dest.streamid");
		  System.out.println(res);
	 }
	 
	 //@Test
	 public void testGenericAggregation() throws JsonProcessingException, IOException, PopulariotyException {
		 
		  Map<String,String> map = new HashMap<String, String>();
		  Query q = new Query(QueryType.AGGREGATIONS);
		  q.addCriteria(new SearchCriteria<String>("doc.src.soid",  "1426859511066689111ae7e5e46dca7066ec2bc59b3b5", SearchCriteriaType.MUST_MATCH));
		  Query internal = new Query(QueryType.AGGREGATIONS);
		  internal.addCriteria(new AggregationCriteria<String>("couchbaseDocument.doc.dest.streamid",null,AggregationCriteriaType.TERMS));
		  q.addSubQuery(internal);
		  QueryResponse resp = search.execute(q, "");
		  System.out.println(resp.getMapResult());
	 }
	 
	 //@AfterClass
	 public static void close()
	 {
		 try {
			/* if(store != null)
				 store.close(null);
			 else 
				 System.err.println("store was already null while tearing down test!");*/
			 
			 if(search != null)
				 search.close(null);
			 else 
				 System.err.println("search was already null while tearing down test!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	 }
}
