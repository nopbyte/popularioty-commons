package popularioty.commons.test.settings;

import java.util.HashMap;
import java.util.Map;

public class Settings {

	private Map<String, String> settings; 
	
	public Settings()
	{
		settings = new HashMap<>();
		settings.put("storage.engine","couch_base");
		settings.put("couchbase.host","[\"192.168.56.101\"]");
		settings.put("couchbase.timeout.value","2");
		settings.put("couchbase.timeout.timeunit","2");		
		//settings.put("couchbase.port","8092");
		settings.put("feedback.bucket","feedback");
		settings.put("search.engine","elastic_search");
		settings.put("client.transport.host","[\"192.168.56.101:9300\"]");
		settings.put("client.transport.clustername","serviolastic");
		settings.put("client.transport.port","9300");
		settings.put("index.aggregated","reputation_aggregations");
		settings.put("index.feedback","feedback");
		settings.put("index.metafeedback","meta_feedback");
		settings.put("index.subreputation","subreputation");
		settings.put("index.runtime","servioticy-popularioty");
		
	}
	public String getProperty(String value)
	{
		return settings.get(value);
	}
	public Map getSettings()
	{
		return settings;
	}
}
