
public class Infrastructure {
	private String _type;
	private String _name;
	private int _semanticEntityId;
	private int _regionId;
	
	public Infrastructure(String _type, String _name, int _semanticEntityId, int _regionId) {
		this._type = _type;
		this._name = _name;
		this._semanticEntityId = _semanticEntityId;
		this._regionId = _regionId;
	}
	
	public String get_type() {
		return _type;
	}
	public void set_type(String _type) {
		this._type = _type;
	}
	public String get_name() {
		return _name;
	}
	public void set_name(String _name) {
		this._name = _name;
	}
	public int get_semanticEntityId() {
		return _semanticEntityId;
	}
	public void set_semanticEntityId(int _semanticEntityId) {
		this._semanticEntityId = _semanticEntityId;
	}
	public int get_regionId() {
		return _regionId;
	}
	public void set_regionId(int _regionId) {
		this._regionId = _regionId;
	}
	
	
	
}
