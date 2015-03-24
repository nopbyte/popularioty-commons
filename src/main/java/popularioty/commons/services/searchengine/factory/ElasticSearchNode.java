package popularioty.commons.services.searchengine.factory;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.Fuzziness;
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
	private int prop_transport_port;
	private Node node;
	private  Client client;
	private boolean closed = false;

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
	 
	@Override
	public void init(Map<String, Object> configuration) throws Exception {
		
		//if(configuration.containsKey("client.transport.host") && configuration.containsKey("client.transport.port"))
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

	public static Fuzziness getFuzziness(int levenshtein) 
	{
		if(levenshtein==0)
			return Fuzziness.ZERO;
		if(levenshtein==1)
			return Fuzziness.ONE;
		if(levenshtein==2)
			return Fuzziness.TWO;
		return Fuzziness.AUTO;
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
		return executeAggregation(query, null);
	}

}
