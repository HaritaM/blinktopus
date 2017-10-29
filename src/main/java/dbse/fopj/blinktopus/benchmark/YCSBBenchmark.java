package dbse.fopj.blinktopus.benchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import javax.ws.rs.core.Response.Status;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */

public class YCSBBenchmark {
	  
	  public Status read(String table, String key, Set<String> fields,
		 Map<String, Map<String, String>> result) throws IOException {
		 
		  String url = "http://localhost:8080/readLog?SvId=primary&type=log&table=User&key="+key+"&";
		  if (fields!=null){
			  for (String field: fields){
			  	url+="fields="+field+"&";
		  	}
		  }
		  url = url.substring(0, url.length()-1);
		  
		  URL obj = new URL(url);
		  HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		  con.setRequestMethod("GET");
		  con.setRequestProperty("Accept", "application/json");	  
		  BufferedReader in = new BufferedReader(
		  new InputStreamReader(con.getInputStream()));
		  String inputLine;
		  ObjectMapper mapper=new ObjectMapper();
		  
		  while ((inputLine = in.readLine()) != null) {
			Map<String,String> object=new HashMap<String,String>();
			object=mapper.readValue(inputLine,new TypeReference<Map<String,String>>(){});
			result.put((String)object.get("key"),object);
		  }
		  in.close();
		  
		  return result.isEmpty() ? Status.INTERNAL_SERVER_ERROR : Status.OK;
	  }

	  public Status insert(String table, String key,
	      Map<String, Map<String, String>> values) {
		  String url = "http://localhost:8080/insertLog?SvId=primary&type=log&table=User&key="+key+"&";
		  for (Map.Entry<String, Map<String, String>> object : values.entrySet()){
			  for (Map.Entry<String, String> field : object.getValue().entrySet()){
				  url+=field.getKey()+"="+field.getValue()+"&";  
			  }
		  }
		  url = url.substring(0, url.length()-1);
		  URL obj;
		  HttpURLConnection con = null;
		  int respStatus = Status.INTERNAL_SERVER_ERROR.getStatusCode();
		try {
			obj = new URL(url);
			con = (HttpURLConnection) obj.openConnection();
			con.setRequestMethod("GET");
			con.setRequestProperty("Accept", "application/json");	  
			BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
			}
			in.close();
			respStatus = con.getResponseCode();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		  
	    return respStatus==Status.INTERNAL_SERVER_ERROR.getStatusCode()?Status.INTERNAL_SERVER_ERROR:respStatus==201?Status.CREATED:Status.BAD_REQUEST;
	  }

	
	  public Status delete(String table, String key) throws IOException  {
		  String url = "http://localhost:8080/deleteLog?SvId=primary&type=log&table=User&key="+key;
		  URL obj = new URL(url);
		  HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		  con.setRequestMethod("GET");
		  con.setRequestProperty("Accept", "application/json");	  
		  BufferedReader in = new BufferedReader(
		  new InputStreamReader(con.getInputStream()));
		  String inputLine;
		  while ((inputLine = in.readLine()) != null) {
		  }
		  in.close();
		
	    return con.getResponseCode()==201?Status.OK:Status.BAD_REQUEST;
	  }

	  public Status update(String table, String key,
	      Map<String, Map<String, String>> values) throws IOException {
		  String url = "http://localhost:8080/updateLog?SvId=primary&type=log&table=User&key="+key+"&";
		  for (Map.Entry<String, Map<String, String>> object : values.entrySet()){
			  for (Map.Entry<String, String> field : object.getValue().entrySet()){
				  url+=field.getKey()+"="+field.getValue()+"&";  
			  }
		  }
		  url = url.substring(0, url.length()-1);
		  URL obj = new URL(url);
		  HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		  con.setRequestMethod("GET");
		  con.setRequestProperty("Accept", "application/json");	  
		  BufferedReader in = new BufferedReader(
		  new InputStreamReader(con.getInputStream()));
		  String inputLine;
		  while ((inputLine = in.readLine()) != null) {
		  }
		  in.close();
		
	    return con.getResponseCode()==201?Status.OK:Status.BAD_REQUEST;
	  }

	  public Status scan(String table, String startkey, int recordcount,
	      Set<String> fields, Vector<HashMap<String, Map<String, String>>> result) throws JsonParseException, JsonMappingException, IOException {
			 	 
				  String url = "http://localhost:8080/scanLog?SvId=primary&type=log&table=User&key="+startkey+"&recordCount="+recordcount+"&";
				  if (fields!=null){
					  for (String field: fields){
					  	url+="fields="+field+"&";
				  	}
				  }
				  url = url.substring(0, url.length()-1);
				  
				  URL obj = new URL(url);
				  HttpURLConnection con = (HttpURLConnection) obj.openConnection();
				  con.setRequestMethod("GET");
				  con.setRequestProperty("Accept", "application/json");	  
				  BufferedReader in = new BufferedReader(
				  new InputStreamReader(con.getInputStream()));
				  String inputLine;
				  ObjectMapper mapper=new ObjectMapper();
				  
				  while ((inputLine = in.readLine()) != null) {
					List<Map<String,String>> object=new ArrayList<>();
					object=mapper.readValue(inputLine,new TypeReference<List<Map<String,String>>>(){});
					System.out.println("Object size: "+object.size());
					for (Map<String,String> entry: object){
						HashMap<String,Map<String,String>> base=new HashMap<String,Map<String,String>>();
						base.put((String)entry.get("key"),entry);
						result.addElement(base);
					}

				  }
				  in.close();
				  
				  return result.isEmpty() ? Status.INTERNAL_SERVER_ERROR : Status.OK;
		
	  }

	public static void main (String args[]) throws IOException{
		YCSBBenchmark test = new YCSBBenchmark();
		Map<String, Map<String, String>> values = new HashMap<>();
		Map<String, String> object = new HashMap<>();
		object.put("key", "10");
		object.put("field0", "5");
		object.put("field1", "5");
		object.put("field2", "5");
		object.put("field3", "5");
		object.put("field4", "5");
		object.put("field5", "5");
		object.put("field6", "5");
		object.put("field7", "5");
		object.put("field8", "5");
		object.put("field9", "5");
		values.put("10", object);
		test.insert("User", "10", values);
	}
}
