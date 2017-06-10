import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class StreamServer {
	private Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:derby:sysdata;create=True");
    }

	public void createAndFillDatabase(){
        try {
        	Connection conn = getConnection();
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
                conn.close();
            }
        }catch (IOException | SQLException e) {
			e.printStackTrace();
		}finally{
			//closeConnection();
		}
    }
	
	public static void main(String[] args) throws Exception {
        int clientId = 0;
        ServerSocket listener = new ServerSocket(8200);
        StreamServer ss = new StreamServer();
        ss.createAndFillDatabase();
        System.out.println("Server is running at port 8200.");
        try {
            while (true) {
                new StreamProcessor(listener.accept(), clientId++).start();
            }
        } finally {
            listener.close();
        }
    }

	private static class StreamProcessor extends Thread{
		private Socket socket;
        private int clientId;
        private Connection conn=null;
        private PrintWriter out = null;
        private Map<String, VariableCollection> symbolTable = null;
        
        public StreamProcessor(Socket socket, int clientid) {
            this.socket = socket;
            this.clientId = clientid;
            System.out.println("New client " + clientId + " at " + socket);
        }
        
    	
    	public void cleanUp(){
    		try {
    			Statement st = conn.createStatement();
    			for(String s: symbolTable.keySet()){
    				if(symbolTable.get(s).get_varType().compareTo("sensorcollection") == 0){
    					ResultSet tables = conn.getMetaData().getTables(null, null, s.toUpperCase()+clientId, null);
    					if(tables.next())
    						st.executeUpdate("drop table " + s+ clientId);
    					tables.close();
    				}
    			}
    			st.close();
    		} catch (SQLException e) {
    			e.printStackTrace();
    		}
    	}
    	
    	public boolean validTuple(Observation o, String tableName){
    		String sensorCollection = symbolTable.get(tableName).get_observationMapping();
    		boolean returnValue = false;
    		try {
    			Statement s = conn.createStatement();
    			ResultSet values = s.executeQuery("select * from " + sensorCollection +clientId+ " where id = '" + o.sensor_id + "'");
    			if(values.next()){
    				returnValue = true;
    			}
    			values.close();
    		} catch (SQLException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
    		return returnValue;
    	}
    	
    	public String processWindow(List<Observation> window, String[] tokens, List<String> queryList){
    		StringBuilder result = new StringBuilder();
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
    				
    				//System.out.println(ret.toString());
    				result.append(ret.toString());
    			}else{
    				result.append("{ count: " + String.valueOf(window.size()) + "}");
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
    					result.append(ret.toString());
    					//System.out.println(result);
    				}
    			}else{
    				Optional<Observation> ret = window.stream().collect(Collectors.minBy(comp));
    				result.append(ret.isPresent()? ret.toString():"");
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
    					result.append(ret.toString());
    					//System.out.println(result);
    				}
    			}else{
    				Optional<Observation> ret = window.stream().collect(Collectors.maxBy(comp));
    				result.append(ret.isPresent()? ret.toString():"");
    			}
    		}else if(tokens[1].compareTo("*") == 0){
    			result.append(window.toString());
    		}else{
    			String[] attributes = tokens[1].split(",");
    			result.append("{\n");
    			for(Observation o : window){
    				result.append("{");
    				for(String s : attributes){
    					try {
    						result.append(s + " : " + o.getClass().getField(s).get(o).toString() + ",");
    					} catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException
    							| SecurityException e) {
    						// TODO Auto-generated catch block
    						e.printStackTrace();
    					}
    				}
    				result.setLength(result.length() - 1);
    				result.append("}\n");
    			}
    			result.append("}");
    		}
    		return result.toString();
    	}
    	
    	
    	public int runQuery(String[] tokens){
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
    				List<Observation> window = new ArrayList<Observation>();
    				while(true){
    					while (tuplesReadSoFarCount < rangeValue && (line = br.readLine()) != null) {
    	
    					    String[] obs = line.split(",");
    					    Observation o = new Observation(obs);
    					    if(validSensorIds.contains(o.sensor_id) || validTuple(o,tableName)){
    					    	window.add(o);
    					    	validSensorIds.add(o.sensor_id);
    					    	tuplesReadSoFarCount++;
    					    }
    					}
    					String result = processWindow(window, tokens, tok);
    					out.println(result);
    					if(slideValue == -1 || line == null){
    						break;
    					}else{
    						if(slideValue <= rangeValue){
    							tuplesReadSoFarCount -= slideValue;
    							window.subList(0, slideValue).clear();
    						}
    					}
    				}
    				br.close();
    			}else if(rangeUnit.toLowerCase().compareTo("minutes") == 0){
    				String startTime = "6/6/2017 09:54:18";
    				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy HH:mm:ss");
    				LocalDateTime dt = LocalDateTime.parse(startTime, formatter);
    				
    				List<Observation> window = new ArrayList<Observation>();
    				while(true){
    					while ((line = br.readLine()) != null) {
    	
    					    // use comma as separator
    					    String[] obs = line.split(",");
    					    Observation o = new Observation(obs);
    					    LocalDateTime tupleTime = LocalDateTime.parse(o.timestamp, formatter);
    					    if(ChronoUnit.SECONDS.between(dt, tupleTime) > rangeValue*60){
    					    	break;
    					    }
    					    if(validSensorIds.contains(o.sensor_id) || validTuple(o,tableName)){
    					    	window.add(o);
    					    	validSensorIds.add(o.sensor_id);
    					    }
    					}
    					String result = processWindow(window, tokens, tok);
    					out.println(result);
    					if(slideValue == -1 || line == null){
    						break;
    					}else{
    						if(slideValue <= rangeValue){
    							for (Iterator<Observation> iterator = window.iterator(); iterator.hasNext();) {
    							    Observation obs = iterator.next();
    							    LocalDateTime tupleTime = LocalDateTime.parse(obs.timestamp, formatter);
    							    if (ChronoUnit.SECONDS.between(dt, tupleTime) <= slideValue*60) {
    							        // Remove the current element from the iterator and the list.
    							        iterator.remove();
    							    }else{
    							    	break;
    							    }
    							}
    							dt = LocalDateTime.parse(window.get(0).timestamp, formatter);
    						}
    					}
    				}
    				br.close();
    			}
    			
    		} catch (Exception e) {
    			e.printStackTrace();
    			cleanUp();
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
    			symbolTable.put(tokens[i], vc);
    		}
    		out.println("Variable defined of type: " + varType);
    	}
    	
    	public int assignment(String[] tokens){
    		String varName = tokens[0];
    		varName = varName.replaceAll("\\s+","");
    		int returnValue = 1;
    		if(symbolTable.get(varName).get_varType() == "sensorcollection"){
    			try {
    				Statement s = conn.createStatement();
    				ResultSet tables = conn.getMetaData().getTables(null, null, varName.toUpperCase()+clientId, null);
					if(tables.next())
						s.executeUpdate("drop table " + varName+ clientId);
					tables.close();
    				s.addBatch("create table " + varName +clientId +" as " + tokens[1]+ " with no data");
    				s.addBatch("insert into " + varName + clientId + " " + tokens[1]);
    				s.executeBatch();
    				conn.commit();
    				s.close();
    				out.println("Assignment Query executed successfully.");
    			} catch (SQLException e) {
    				e.printStackTrace();
    				cleanUp();
    			}
    		}else if(symbolTable.get(varName).get_varType() == "observationstream"){
    			if(tokens[1].toUpperCase().contains("SENSORS_TO_OBSERVATION_STREAM")){
    				symbolTable.get(varName).set_observationMapping(tokens[1].substring(tokens[1].indexOf("(")+1, tokens[1].indexOf(")")));
    				out.println("Assignment Query executed successfully.");
    			}else
    				returnValue = -1;
    		}
    		
    		return returnValue;
    	}
    	
    	public int process(String query){
    		String[] tokens = query.split(" ");
    		int returnValue = 1;
    		if(tokens == null){
    			returnValue = -1;
    		}else if(tokens[0].toLowerCase().compareTo("select") == 0){
    			returnValue = runQuery(tokens);
    		}else if(tokens[0].toLowerCase().compareTo("define") == 0){
    			//declaration
    			define(tokens, 1);
    		}else if(tokens.length > 0 && tokens[1].compareTo("=") == 0){
    			//assignment
    			returnValue = assignment(query.split("=", 2));
    		}else{
    			out.println("Incorrect query");
    			returnValue = -1;
    		}
    		return returnValue;
    	}
    	
    	
        public void run() {
        	symbolTable = new HashMap<String, VariableCollection>();
            try {
            	conn = DriverManager.getConnection("jdbc:derby:sysdata;create=true");;
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
                out = new PrintWriter(socket.getOutputStream(), true);

                while (true) {
                    String query = in.readLine();
                    if (query == null || query.equals(".")) {
                        break;
                    }
                    out.println(query);
                    System.out.println(query);
                    process(query);
                }
            } catch (IOException | SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                	cleanUp();
                	if(conn != null){
                		conn.close();
                		conn = null;
                	}
                    socket.close();
                } catch (IOException | SQLException e) {
                    System.out.println("Failed to close socket.");
                }
                System.out.println("Client " + clientId + " disconnected.");
            }
        }

        
	}
}
