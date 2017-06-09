import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class VariableCollection {
	private String _varName;
	private ResultSet _values;
	private String _varType;
	private String _observationMapping;
	
	public String get_observationMapping() {
		return _observationMapping;
	}

	public void set_observationMapping(String _observationMap) {
		this._observationMapping = _observationMap;
	}

	public VariableCollection(String vn, String vt){
		_varName = vn;
		_varType = vt;
		_values = null;
	}

	public String get_varType() {
		return _varType;
	}

	public void set_varType(String _varType) {
		this._varType = _varType;
	}

	public String get_varName() {
		return _varName;
	}

	public void set_varName(String _varName) {
		this._varName = _varName;
	}

	public ResultSet get_values() {
		return _values;
	}

	public void set_values(ResultSet _values) {
		this._values = _values;
	}
}
