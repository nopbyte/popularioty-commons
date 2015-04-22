package popularioty.commons.services.searchengine.factory;


import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import popularioty.commons.exception.PopulariotyException;
import popularioty.commons.exception.PopulariotyException.Level;
import popularioty.commons.services.searchengine.elasticsearch.ElasticSearchAdapter;
import popularioty.commons.services.searchengine.queries.Query;
import popularioty.commons.services.searchengine.queries.QueryResponse;
import popularioty.commons.services.searchengine.queries.QueryResponse.QueryResponseType;
import popularioty.commons.services.searchengine.queries.QueryType;
import popularioty.commons.services.storageengine.factory.StorageProvider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Although implementing the locking mechanism for each class extending this SearchNode is not the most elegant, it was preffered
 * because it preserves encaplsulation.
 * @author dp
 *
 */
public  class ElasticSearchNode extends ElasticSearchAdapter implements SearchProvider {

	private static Logger LOG = LoggerFactory.getLogger(ElasticSearchNode.class);	
	private String prop_transport_host;
	private Node node;
	private  Client client;
	private boolean closed = false;

	/**
	 * Gets all the documents from a given list of Ids
	 * @param res QueryResponse
	 * @param index index to be used to retrieve the data
	 * @return List of Maps containing all the documents
	 * @throws PopulariotyException
	 */
	private QueryResponse retrieveFullData(QueryResponse res,String index) throws PopulariotyException
	{
		if(store != null &&  res.getQueryResponsetype().equals(QueryResponseType.LIST_OF_STRINGS)) 
		{
			List<Map<String,Object>> docs =  new LinkedList<Map<String,Object>>();
			docs  = store.getData(res.getListofStringsResult(), index, false);
			res.setQueryResponsetype(QueryResponseType.LIST_OF_MAPS);
			res.setListofMapsResult(docs);
		}
		return res;
	}
	
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
	 *  Receives a string contining a JSON array of urls of host:port pointing to ES nodes in the cluster under the property client.transport.host
	 *  Also the StorageProvider used to ge the data by ID must be provided with the key storage.provider.object
	 */
	@Override
	public void init(Map<String, Object> configuration) throws Exception {
		
		if(configuration.containsKey("storage.provider.object"))
			store = (StorageProvider) configuration.get("storage.provider.object");
		//{
		this.prop_transport_host= (String) configuration.get("client.transport.host");
		//}//readProperties(properties);
		List<String> hosts = parseHosts(prop_transport_host);
		if(configuration.containsKey("client.transport.clustername"))
		{
			Settings settings = ImmutableSettings.settingsBuilder()
		        .put("cluster.name", configuration.get("client.transport.clustername")).build();
			client = new TransportClient(settings);
		}
		else{
		    client = new TransportClient();
		}
		for(String host: hosts)
		{
			String arr[] = host.split(":");
		    client = ((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(arr[0], (arr.length>1?Integer.parseInt(arr[1]):9300)));
		}
	
  		LOG.info("Initializing the elasticsearch node connection...");
  		closed = false;
	}


	@Override
	public void close(Map<String, Object> configuration) throws Exception {
		
		if(node != null && !closed)
		 {
			  node.close();
			  node = null;
			  client = null;
			  closed = true;
			  LOG.info("Closing the elasticsearch node connection!");
		 }
		LOG.info("Attempting to close elasticsearch node, but it was already closed...");
		
	}

	
	public Map<String,Long> getCountOfDocumentsByTerm(Map<String,String> mustMatchCriteria, String term)
	{
		FilterAggregationBuilder built = AggregationBuilders
	    .filter("agg");
		for(String key: mustMatchCriteria.keySet())
		{
			built.filter(FilterBuilders.termFilter(key ,mustMatchCriteria.get(key) ));
		}
		built.subAggregation(AggregationBuilders.terms("agg_term").field(term));
		
		SearchResponse sr = client.prepareSearch().addAggregation(built).execute().actionGet();
		Filter agg = sr.getAggregations().get("agg");
		Terms term_r = agg.getAggregations().get("agg_term");
		Map<String,Long> termsresult = new HashMap<String, Long>();
		
		for (Terms.Bucket entry: term_r.getBuckets()) {
			termsresult.put(entry.getKey(),  entry.getDocCount());
		}
		return termsresult;
	}
	
	public QueryResponse execute(Query query, String index) throws PopulariotyException
	{
		QueryResponse  res = null;
		try{
			if(query.getType().equals(QueryType.SELECT)|| query.getType().equals(QueryType.SELECT_ID))
			{
				res = super.executeSelect(query, index,true);
				if(store != null &&  query.getType().equals(QueryType.SELECT) && res.getQueryResponsetype().equals(QueryResponseType.LIST_OF_STRINGS)) 
				{
					res = retrieveFullData(res, index);				
				}
			}
			else if(query.getType().equals(QueryType.AGGREGATIONS))
				res = executeAggregation(query, null);
			else if (query.getType().equals(QueryType.FUZZY_TEXT_SEARCH))
				res = retrieveFullData(executeFuzzyTextSearch(query, index,true),index);
			
		}catch(SearchPhaseExecutionException se)
		{
			if(se.phaseName().equals("query"))
				throw new PopulariotyException("Search error",null,LOG,"Unable to execute query in ElasticSearch. SearchPhaseExecutionException with phasename == query. Index is empty?",Level.DEBUG,500);
		}
		return res;
		
	}
	
	// Additional methods specific for elasticsearch node 
	//TODO needs to be removed after the refactoring has been done!
	public Node getNode() {
		return node;
	}


	public void setNode(Node node) {
		this.node = node;
	}


	public Client getClient() {
		return client;
	}


	public void setClient(Client client) {
		this.client = client;
	}

	public static Map<String, Object> addId(SearchHit hit, String idField) {
		Map<String, Object> tmp;
		tmp = hit.getSource();
		tmp.put(idField, hit.getId());
		return tmp;
	}



}
