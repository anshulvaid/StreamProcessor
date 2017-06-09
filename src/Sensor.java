
public class Sensor {
	private String _id;
	private String _description;
	private String _name;
	private String _sensorIp;
	private String _sensorPort;
	private int _coverageId;
	private int _userId;
	private int _locationId;
	private String _platformId;
	private String _sensorTypeId;
	
	
	public Sensor(String _id, String _description, String _name, String _sensorIp, String _sensorPort, int _coverageId,
			int _userId, int _locationId, String _platformId, String _sensorTypeId) {
		this._id = _id;
		this._description = _description;
		this._name = _name;
		this._sensorIp = _sensorIp;
		this._sensorPort = _sensorPort;
		this._coverageId = _coverageId;
		this._userId = _userId;
		this._locationId = _locationId;
		this._platformId = _platformId;
		this._sensorTypeId = _sensorTypeId;
	}
	
	public String get_id() {
		return _id;
	}
	public void set_id(String _id) {
		this._id = _id;
	}
	public String get_description() {
		return _description;
	}
	public void set_description(String _description) {
		this._description = _description;
	}
	public String get_name() {
		return _name;
	}
	public void set_name(String _name) {
		this._name = _name;
	}
	public String get_sensorIp() {
		return _sensorIp;
	}
	public void set_sensorIp(String _sensorIp) {
		this._sensorIp = _sensorIp;
	}
	public String get_sensorPort() {
		return _sensorPort;
	}
	public void set_sensorPort(String _sensorPort) {
		this._sensorPort = _sensorPort;
	}
	public int get_coverageId() {
		return _coverageId;
	}
	public void set_coverageId(int _coverageId) {
		this._coverageId = _coverageId;
	}
	public int get_userId() {
		return _userId;
	}
	public void set_userId(int _userId) {
		this._userId = _userId;
	}
	public int get_locationId() {
		return _locationId;
	}
	public void set_locationId(int _locationId) {
		this._locationId = _locationId;
	}
	public String get_platformId() {
		return _platformId;
	}
	public void set_platformId(String _platformId) {
		this._platformId = _platformId;
	}
	public String get_sensorTypeId() {
		return _sensorTypeId;
	}
	public void set_sensorTypeId(String _sensorTypeId) {
		this._sensorTypeId = _sensorTypeId;
	}
	
	
}
