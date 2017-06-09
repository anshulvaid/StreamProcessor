import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.stream.*;

public class StreamProcessor {
	private Map<String,VariableCollection> _symbolTable;
	private Connection conn;
	
	public StreamProcessor(){
		_symbolTable = new HashMap<String,VariableCollection>();
		this.createAndFillDatabase();
	}
	
	private Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:derby:sysdata;create=True");
    }

	
	public void closeConnection(){
		try
        {
            DriverManager.getConnection("jdbc:derby:;shutdown=true");
        }
        catch (SQLException se)
        {
            if (( (se.getErrorCode() == 50000)
                    && ("XJ015".equals(se.getSQLState()) ))) {
                System.out.println("Derby shut down normally");
            } else {
                System.err.println("Derby did not shut down normally");
                se.printStackTrace();
            }
        }
	}
	
	public void cleanUp(){
		try {
			Statement st = conn.createStatement();
			for(String s: _symbolTable.keySet()){
				if(_symbolTable.get(s).get_varType().compareTo("sensorcollection") == 0){
					st.executeUpdate("drop table "+ s.toUpperCase());
				}
			}
			st.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private void createAndFillDatabase(){
        try {
        	conn = getConnection();
            ResultSet tables = conn.getMetaData().getTables(null, null, "SENSOR", null);
            if (!tables.next()) {
                // create table
                Statement stmt = conn.createStatement();
                stmt.execute("CREATE TABLE sensor(\n"
                        + "  id VARCHAR(50) NOT NULL PRIMARY KEY\n"
                        + ", description VARCHAR(50)\n"
                        + ", name VARCHAR(50)\n"
                        + ", sensorIP VARCHAR(50)\n"
                        + ", sensorPort VARCHAR(10)\n"
                        + ", COVERAGE_ID VARCHAR(10)\n"
                        + ", LOCATION_ID VARCHAR(10)\n"
                        + ", USER_ID VARCHAR(10)\n"
                        + ", PLATFORM_ID VARCHAR(10)\n"
                        + ", sensor_type_id VARCHAR(5)\n"
                        + ")");
                // insert data
                BufferedReader br = new BufferedReader(new FileReader(new File("sensors.csv")));
                String line;
                PreparedStatement pstmt = conn.prepareStatement("INSERT INTO sensor(id, description, name,"
                		+"sensorIP, sensorPort, COVERAGE_ID, LOCATION_ID, USER_ID, PLATFORM_ID,sensor_type_id)"
                		+"VALUES (?,?,?,?,?,?,?,?,?,?)");
                while((line = br.readLine()) != null){
        			String[] entries = line.split(",");
        			pstmt.setString(1, entries[0]);
        			pstmt.setString(2, entries[1]);
        			pstmt.setString(3, entries[2]);
        			pstmt.setString(4, entries[3]);
        			pstmt.setString(5, entries[4]);
        			pstmt.setString(6, entries[5]);
        			pstmt.setString(7, entries[6]);
        			pstmt.setString(8, entries[7]);
        			pstmt.setString(9, entries[8]);
        			pstmt.setString(10, entries[9]);
        			pstmt.execute();
        		}
                br.close();
                pstmt.close();
                conn.commit();
            }
            tables = conn.getMetaData().getTables(null, null, "INFRASTRUCTURE", null);
            if (!tables.next()) {
                // create table
                Statement stmt = conn.createStatement();
                stmt.execute("CREATE TABLE infrastructure(\n"
                        + "  name VARCHAR(20)\n"
                        + ", type VARCHAR(20)\n"
                        + ", SEMANTIC_ENTITY_ID VARCHAR(10)\n"
                        + ", REGION_ID VARCHAR(5)\n"
                        + ")");
                // insert data
                BufferedReader br = new BufferedReader(new FileReader(new File("infra.csv")));
                String line;
                PreparedStatement pstmt = conn.prepareStatement("INSERT INTO infrastructure(name,"
                		+"type, SEMANTIC_ENTITY_ID, REGION_ID)"
                		+"VALUES (?,?,?,?)");
                while((line = br.readLine()) != null){
        			String[] entries = line.split(",");
        			pstmt.setString(1, entries[0]);
        			pstmt.setString(2, entries[1]);
        			pstmt.setString(3, entries[2]);
        			pstmt.setString(4, entries[3]);
        			pstmt.execute();
        		}
                br.close();
                pstmt.close();
                conn.commit();
            }
        }catch (IOException | SQLException e) {
			e.printStackTrace();
			closeConnection();
		}
    }
	
	public boolean validTuple(Observation o, String tableName){
		String sensorCollection = _symbolTable.get(tableName).get_observationMapping();
		try {
			Statement s = conn.createStatement();
			ResultSet values = s.executeQuery("select * from "+ sensorCollection.toUpperCase() + " where id = '" + o.sensor_id + "'");
			if(values.next()){
				return true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	public String processWindow(List<Observation> window, String[] tokens, List<String> queryList){
		String result = null;
		boolean grouped = false;
		String groupByCol = null;
		if(queryList.contains("GROUP")){
			grouped = true;
			groupByCol = tokens[queryList.indexOf("GROUP") + 2];
		}
		final String col = groupByCol;
		if(tokens[1].toLowerCase().contains("count")){
			if(grouped){
				Map<Object, Long> ret =  window.stream().collect(Collectors.groupingBy(w->{
					try {
						return w.getClass().getMethod("get" + col).invoke(w);
					} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
						e.printStackTrace();
					}
					return null;
				}, Collectors.counting()));
				
				System.out.println(ret.toString());
			}else{
				result = "{ count: " + String.valueOf(window.size()) + "}";
			}
		}else if(tokens[1].toLowerCase().contains("min")){
			String aggCol = tokens[1].substring(tokens[1].indexOf("(")+1, tokens[1].indexOf(")"));
			Comparator<Observation> comp = new Comparator<Observation>(){

				@Override
				public int compare(Observation o1, Observation o2) {
					try {
						Field toCompare = o1.getClass().getField(aggCol);
						Object v1 = toCompare.get(o1);
						Object v2 = toCompare.get(o2);
						Comparable c1 = (Comparable)v1;
			            Comparable c2 = (Comparable)v2;
						return c1.compareTo(c2);
					} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return 0;
				}};
			if(grouped){
				if(aggCol.compareTo("*") == 0){
					System.out.println("Incorrect query format");
				}else{
					Map<Object, Optional<Observation>> ret =  window.stream().collect(Collectors.groupingBy(w->{
						try {
							return w.getClass().getMethod("get" + col).invoke(w);
						} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
							e.printStackTrace();
						}
						return null;
					}, Collectors.minBy(comp)));
					result = ret.toString();
					System.out.println(result);
				}
			}else{
				Optional<Observation> ret = window.stream().collect(Collectors.minBy(comp));
				result = ret.isPresent()? ret.toString():"";
			}
		}else if(tokens[1].toLowerCase().contains("max")){
			String aggCol = tokens[1].substring(tokens[1].indexOf("(")+1, tokens[1].indexOf(")"));
			Comparator<Observation> comp = new Comparator<Observation>(){

				@Override
				public int compare(Observation o1, Observation o2) {
					try {
						Field toCompare = o1.getClass().getField(aggCol);
						Object v1 = toCompare.get(o1);
						Object v2 = toCompare.get(o2);
						Comparable c1 = (Comparable)v1;
			            Comparable c2 = (Comparable)v2;
						return c1.compareTo(c2);
					} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					return 0;
				}};
			if(grouped){
				if(aggCol.compareTo("*") == 0){
					System.out.println("Incorrect query format");
				}else{
					Map<Object, Optional<Observation>> ret =  window.stream().collect(Collectors.groupingBy(w->{
						try {
							return w.getClass().getMethod("get" + col).invoke(w);
						} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
							e.printStackTrace();
						}
						return null;
					}, Collectors.maxBy(comp)));
					result = ret.toString();
					System.out.println(result);
				}
			}else{
				Optional<Observation> ret = window.stream().collect(Collectors.maxBy(comp));
				result = ret.isPresent()? ret.toString():"";
			}
		}else if(tokens[1].compareTo("*") == 0){
			result = window.toString();
		}else{
			String[] attributes = tokens[1].split(",");
			StringBuilder res = new StringBuilder("{");
			for(Observation o : window){
				for(String s : attributes){
					try {
						res.append(s + " : " + o.getClass().getField(s).get(o).toString() + ",");
					} catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException
							| SecurityException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				res.setLength(res.length() - 1);
				res.append("}\n");
			}
			result = res.toString();
			System.out.println(result);
		}
		return result;
	}
	
	
	public int run(String[] tokens){
		int returnValue = 1;
		List<String> tok = Arrays.asList(tokens);
		int rangeIndex = tok.indexOf("RANGE");
		int slideIndex = tok.indexOf("SLIDE");
		String rangeUnit = tokens[rangeIndex + 2].replaceAll("'", "");
		int rangeValue = Integer.parseInt(tokens[rangeIndex + 1].replaceAll("'", ""));
		int slideValue = -1;
		if(slideIndex != -1){
			slideValue = Integer.parseInt(tokens[slideIndex + 1].replaceAll("'", ""));
		}
		String tableName = tokens[tok.indexOf("FROM") + 1].replaceAll("\\s+","");
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File("observations.csv")));
			String line = "";
			List<String> validSensorIds = new ArrayList<String>();
			if(rangeUnit.toLowerCase().compareTo("tuples") == 0){
				int tuplesReadSoFarCount = 0;
				while(true){
					List<Observation> window = new ArrayList<Observation>();
					while (tuplesReadSoFarCount < rangeValue && (line = br.readLine()) != null) {
	
					    // use comma as separator
					    String[] obs = line.split(",");
					    Observation o = new Observation(obs);
					    if(validSensorIds.contains(o.sensor_id) || validTuple(o,tableName)){
					    	window.add(o);
					    	validSensorIds.add(o.sensor_id);
					    	tuplesReadSoFarCount++;
					    }
					}
					String result = processWindow(window, tokens, tok);
					break;
					//empty top slideValue obs from window, 
					//set tuplesReadSoFarCount -= slideValue
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			cleanUp();
			closeConnection();
		}
		if(rangeUnit.toLowerCase().compareTo("minutes") == 0){
			
		}else {
			
		}
		return returnValue;
	}
	
	public void define(String[] tokens, int offset){
		String varType = "observationstream";
		if(tokens[offset].toLowerCase().compareTo("sensorcollection") == 0){
			varType = "sensorcollection";
		}
		for(int i=offset+1; i<tokens.length; i++){
			VariableCollection vc = new VariableCollection(tokens[i], varType);
			_symbolTable.put(tokens[i], vc);
		}
	}
	
//	public List<Map<String, String>> selection(String[] tokens, int offset, List<String> tableNames, boolean aliased, boolean join, Map<String, String> alias){
//		List<Map<String, String>> result = new ArrayList<Map<String, String>>();
//		List<String> attributes = new ArrayList<String>();
//		for(String s : tableNames){
////			attributes.addAll(getAttributes(s));
//		}
//		return result;
//	}
//	
//	public List<Map<String, String>> query(String[] tokens, int offset){
//		List<Map<String, String>> result = new ArrayList<Map<String, String>>();
//		boolean star = false, join = false, aliased = false;
//		Map<String, String> alias = new HashMap<String, String>();
//		List<String> attributes = new ArrayList<String>();
//		List<String> tableNames = new ArrayList<String>();
//		int i=offset +1;
//		if(tokens[offset + 1] == "*"){
//			star = true;
//			i++;
//		}else{
//			while(tokens[i].toLowerCase() != "from"){
//				attributes.add(tokens[i++]);
//			}
//		}
//		if(tokens[i++].toLowerCase() == "from"){
//			while(i<tokens.length && tokens[i].toLowerCase() != "where"){
//				if(tokens[i].toLowerCase() == "sensor" || tokens[i].toLowerCase() == "infrastructure" || _symbolTable.containsKey(tokens[i])){
//					tableNames.add(tokens[i]);
//				}else{
//					aliased = true;
//					alias.put(tokens[i], tokens[i-1]);
//				}
//				i++;
//			}
//			if(tableNames.size() > 1){
//				join = true;
//			}
//			if(tokens[i].toLowerCase() == "where"){
//				result = selection(tokens, ++i, tableNames, aliased, join, alias);
//			}
//		}else{
//			System.out.println("Error: Incorrect Query format");
//			result = null;
//		}
//		return result;
//	}
	
	public int assignment(String[] tokens){
		String varName = tokens[0];
		varName = varName.replaceAll("\\s+","");
		int returnValue = 1;
		if(_symbolTable.get(varName).get_varType() == "sensorcollection"){
			try {
				Statement s = conn.createStatement();
				//_symbolTable.get(varName).set_values(s.executeQuery(tokens[1].toUpperCase()));
				s.addBatch("create table "+ varName +" as " + tokens[1]+ " with no data");
				s.addBatch("insert into " + varName + " " + tokens[1]);
				s.executeBatch();
				conn.commit();
				ResultSet r = s.executeQuery("select * from " + varName);
				while(r.next()){
					System.out.println(r.getString("id"));
				}
				s.close();
			} catch (SQLException e) {
				e.printStackTrace();
				cleanUp();
				closeConnection();
			}
		}else if(_symbolTable.get(varName).get_varType() == "observationstream"){
			if(tokens[1].toUpperCase().contains("SENSORS_TO_OBSERVATION_STREAM"))
				_symbolTable.get(varName).set_observationMapping(tokens[1].substring(tokens[1].indexOf("(")+1, tokens[1].indexOf(")")));
		}
		
		return returnValue;
	}
	
	public int process(String query){
		String[] tokens = query.split(" ");
		int returnValue = 1;
		if(tokens == null){
			returnValue = -1;
		}else if(tokens[0].toLowerCase().compareTo("select") == 0){
			returnValue = run(tokens);
		}else if(tokens[0].toLowerCase().compareTo("define") == 0){
			//declaration
			define(tokens, 1);
		}else if(tokens.length > 0 && tokens[1].compareTo("=") == 0){
			//assignment
			returnValue = assignment(query.split("=", 2));
		}else{
			System.out.println("Incorrect query");
			returnValue = -1;
		}
		return returnValue;
	}
	
	public static void main(String args[]){
		System.out.println("**********StreamSQL************");
		System.out.println("Enter Query");
		Scanner sc = new Scanner(System.in);
		StreamProcessor sp = new StreamProcessor();
		while(true){
			String query = sc.nextLine();
			if(query == null || query.isEmpty()){
				break;
			}else{
				System.out.println(query);
				if(sp.process(query) == -1){
					break;
				}
			}
		}
		sp.cleanUp();
		sp.closeConnection();
		sc.close();
		System.out.println("Closed");
		
	}
}
