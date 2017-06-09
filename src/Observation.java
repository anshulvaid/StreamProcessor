
public class Observation {
	public String id;
	public String payload;
	public String timestamp;
	public String sensor_id;
	public String observation_type_id;
	
	public Observation(String[] obs){
		id = obs[0];
		payload = obs[1];
		timestamp = obs[2];
		sensor_id = obs[3];
		observation_type_id = obs[4];
	}
	
	public String getid(){
		return id;
	}
	
	public String getsensor_id(){
		return sensor_id;
	}
	
	public String getpayload(){
		return payload;
	}
	
	public String gettimestamp(){
		return timestamp;
	}
	
	public String getobservation_type_id(){
		return observation_type_id;
	}
	
	@Override
	public String toString(){
		return "{id: " + this.id + ", payload: " + this.payload + ", timestamp: " + this.timestamp + ", sensor_id: " + this.sensor_id
				+ ", observation_type_id: " + this.observation_type_id + "}"; 
	}
}
