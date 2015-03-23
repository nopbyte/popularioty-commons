package popularioty.commons.services.searchengine.criteria;

public abstract class AbstractCriteria <T>
{

	private String field;
	
	private  T value;
		
	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}

	
	protected AbstractCriteria(String field, T value) {
		super();
		this.field = field;
		this.value = value;
	}
	
	
	
	
}
