package popularioty.commons.constants;

public interface EntityTypeConstants 
{
	public static String ENTITY_TYPE_SO_STREAM = "service_object_stream";
	public static String ENTITY_TYPE_SO = "service_object";
	public static String ENTITY_TYPE_SERVICE = "service_instance";
	
	/*
	 * These two are internal
	 */
	public static String ENTITY_TYPE_USER_GIVING_RATING = "user_giving_rating ";
	public static String ENTITY_TYPE_USER_DEVELOPER= "developer";
	// This value is used as type of entity in the reputation databse for the latter two
	public static String ENTITY_TYPE_USER= "user";
	
	public static String REPUTATION_TYPE_ACTIVITY= "activity";
	public static String REPUTATION_TYPE_POPULARITY= "popularity";
	public static String REPUTATION_TYPE_FEEDBACK = "feedback";
	
}
