package popularioty.commons.services.searchengine.factory;


import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Although implementing the locking mechanism for each class extending this SearchNode is not the most elegant, it was preffered
 * because it preserves encaplsulation.
 * @author dp
 *
 */
public  class ElasticSearchNode implements SearchProvider{

	private static Logger LOG = LoggerFactory.getLogger(ElasticSearchNode.class);	
	private String prop_transport_host;
	private int prop_transport_port;
	private Node node;
	private  Client client;
	private boolean closed = false;

	
	@Override
	public void init(Map<String, Object> configuration) throws Exception {
		
		if(configuration.containsKey("client.transport.host") && configuration.containsKey("client.transport.port"))
		this.prop_transport_host= (String) configuration.get("client.transport.host");
  		this.prop_transport_port= Integer.parseInt((String) configuration.get("client.transport.port"));
  		//readProperties(properties);
       
  		//This should be changed if one wants to make the client part of the cluster...
  		client = new TransportClient()
				.addTransportAddress(new InetSocketTransportAddress(prop_transport_host, prop_transport_port));
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
	
}
