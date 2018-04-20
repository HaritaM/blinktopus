package dbse.fopj.blinktopus.resources;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.codahale.metrics.annotation.Timed;

import dbse.fopj.blinktopus.api.datamodel.User;
import dbse.fopj.blinktopus.api.managers.LogManager;
import dbse.fopj.blinktopus.api.managers.SVManager;
import dbse.fopj.blinktopus.api.resultmodel.LogResult;
import dbse.fopj.blinktopus.api.resultmodel.Result;
import dbse.fopj.blinktopus.api.resultmodel.SVManagerResult;

/**
 * Resource class that parses user's query and redirects it either to LogManager or SVManager.
 * @author Pavlo Shevchenko (urmikl18),harita medimi and Ms.c Gabriel Campero
 *
 */
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class QueryProcessor {

	/**
	 * Map that transforms attribute names into numbers. Used to simplify query processing.
	 */
	public final static HashMap<String, Integer> attrIndex=new HashMap<String,Integer>(){

	private static final long serialVersionUID=1L;

	{put("totalprice",0);put("orderdate",1);

	put("linenumber",0);put("quantity",1);put("extendedprice",2);put("discount",3);put("tax",4);put("shipdate",5);put("commitdate",6);put("receiptdate",7);}};

	/**
	 * Default constructor.
	 */
	public QueryProcessor() {

	}

	/**
	 * 
	 * @return List of existing Storage Views , stored in SVManager.
	 */
	@Path("/sv/all")
	@GET
	@Timed
	public SVManagerResult getAllSVs() {
		return SVManager.getSVManager().getAllSV();
	}

	/**
	 * Deletes all stored Storage Views.
	 */
	@Path("/sv/clear")
	@GET
	@Timed
	public void removeAllSVs() {
		SVManager.getSVManager().clear();
	}

	/**
	 * 
	 * @return Returns all the data currently stored in the primary log.
	 */
	@Path("/log")
	@GET
	@Timed
	public LogResult getAllLog() {
		return LogManager.getLogManager().getAllLog();
	}

	/**
	 * The method that returns relevant tuples and relevant information about the query to the user.
	 * @param SVId The ID of a Storage View to be used to answer user's query.
	 * @param type The type of a Storage View to be used to answer user's query (Col,Row,AQP).
	 * @param table The table to be queried (Order/LineItem)
	 * @param attr The attribute to be queried on (e.g. totalprice/extendedprice)
	 * @param lower The lower boundary of a range query.
	 * @param higher The higher boundary of a range query.
	 * @param createSV True, if new SV of given type should be created, false, otherwise.
	 * @param distinct True, if a number of unique values in given range should be returned, false otherwise.
	 * @return An instance of a class Result that contains information about the query (table, attr, lower, higher),
	 * information about SV (Id, Type (Col,Row,AQP)), information about result (tuples, size), and
	 * analytical information (time it took to retrieve the result, and error if necessary).
	 */
	@Path("/query")
	@GET
	@Timed
	public Result query(@QueryParam("SVid") String SVId, @QueryParam("type") String type,
			@QueryParam("table") String table, @QueryParam("attr") String attr, @QueryParam("lower") String lower,
			@QueryParam("higher") String higher, @QueryParam("create") String createSV, @QueryParam("distinct") String distinct) {
		if (type.toLowerCase().equals("log"))
			return LogManager.getLogManager().scan(table, attr, Double.parseDouble(lower), Double.parseDouble(higher),"OK");
		else
		{
			if (type.toLowerCase().equals("aqp"))
				return SVManager.getSVManager().maintain(SVId, type, table, attr, Double.parseDouble(lower),
					Double.parseDouble(higher), false, Boolean.parseBoolean(distinct));
			else
				return SVManager.getSVManager().maintain(SVId, type, table, attr, Double.parseDouble(lower),
						Double.parseDouble(higher), Boolean.parseBoolean(createSV), Boolean.parseBoolean(distinct));
		}
	}
	
	/**
	 * The method that returns relevant tuples and relevant information about the query to the user.
	 * @param SVId The ID of a Storage View to be used to answer user's query.
	 * @param type The type of a Storage View to be used to answer user's query (Col,Row,AQP).
	 * @param table The table to be queried (Order/LineItem)
	 * @param key The key to be queried
	 * @param fields The fields to be queried
	 * @return An instance of a class Result that contains information about the query (table, attr, lower, higher),
	 * information about SV (Id, Type (Col,Row,AQP)), information about result (tuples, size), and
	 * analytical information (time it took to retrieve the result, and error if necessary).
	 */
	@Path("/readLog")
	@GET
	@Timed
	//@ApiOperation(response=dbse.fopj.blinktopus.api.resultmodel.User.class)
	public Response readLog(@QueryParam("SVid") String SVId, @QueryParam("type") String type,
			@QueryParam("table") String table, @QueryParam("key") String key, @QueryParam("fields") List<String> fields) {
		return LogManager.getLogManager().read(table, key, fields, null, null);
	}
	
	/**
	 * The method that returns relevant tuples and relevant information about the query to the user.
	 * @param SVId The ID of a Storage View to be used to answer user's query.
	 * @param type The type of a Storage View to be used to answer user's query (Col,Row,AQP).
	 * @param table The table to be queried (Order/LineItem)
	 * @param key The key to be queried
	 * @param fields The fields to be queried
	 * @param recordCount The number of records to be scanned
	 * @return An instance of a class Result that contains information about the query (table, attr, lower, higher),
	 * information about SV (Id, Type (Col,Row,AQP)), information about result (tuples, size), and
	 * analytical information (time it took to retrieve the result, and error if necessary).
	 */
	@Path("/scanLog")
	@GET
	@Timed
	//@ApiOperation(response=dbse.fopj.blinktopus.api.resultmodel.User.class)
	public Response scanLog(@QueryParam("SVid") String SVId, @QueryParam("type") String type,
			@QueryParam("table") String table, @QueryParam("key") String key, @QueryParam("fields") List<String> fields, @QueryParam("recordCount") Integer recordCount) {
		if (recordCount.equals(null) || recordCount<0 || key.equals(null)){
			return Response.status(Status.BAD_REQUEST).build();
		}
		return LogManager.getLogManager().scan(table, key, fields, recordCount);
	}
	
		
	/**
	 * The method that returns relevant tuples and relevant information about the query to the user.
	 * @param SVId The ID of a Storage View to be used to answer user's query.
	 * @param type The type of a Storage View to be used to answer user's query (Col,Row,AQP).
	 * @param table The table to be queried (Order/LineItem)
	 * @return An instance of a class Result that contains information about the query (table, attr, lower, higher),
	 * information about SV (Id, Type (Col,Row,AQP)), information about result (tuples, size), and
	 * analytical information (time it took to retrieve the result, and error if necessary).
	 */
	@Path("/getAll")
	@GET
	@Timed
	//@ApiOperation(response=dbse.fopj.blinktopus.api.resultmodel.User.class)
	public Response getAll(@QueryParam("SVid") String SVId, @QueryParam("type") String type,
			@QueryParam("table") String table) {
		return LogManager.getLogManager().getAll(table);
	}
	
	/**
	 * The method that loads data into the rowstore.
	 * @return An instance of a class Result.
	 */
	@Path("/loadIntoRows")
	@GET
	@Timed
	//@ApiOperation(response=dbse.fopj.blinktopus.api.resultmodel.User.class)
	public Response loadIntoRows() {
		return LogManager.getLogManager().load();
	}
	
	/**
	 * The method starts the background thread.
	 * @return An instance of a class Result.
	 */
	@Path("/startBT")
	@GET
	@Timed
	//@ApiOperation(response=dbse.fopj.blinktopus.api.resultmodel.User.class)
	public Response startBackgroundThread() {
		return LogManager.getLogManager().startBT();
	}
	
	/**
	 * The method starts the background thread.
	 * @return An instance of a class Result.
	 */
	@Path("/resetBT")
	@GET
	@Timed
	//@ApiOperation(response=dbse.fopj.blinktopus.api.resultmodel.User.class)
	public Response resetBackgroundThread() {
		return LogManager.getLogManager().resetBT();
	}
	/**
	 * This method inserts a tuple of type User in the log. 
	 * @param SVId The ID of a Storage View to be used to answer user's query.
	 * @param type The type of a Storage View to be used to answer user's query (Col,Row,AQP).
	 * @param table The table to be queried (Order/LineItem/user)
	 * @param key
	 * @param field0 
	 * @param field1 
	 * @param field2 
	 * @param field3 
	 * @param field4
	 * @param field5
	 * @param field6
	 * @param field7
	 * @param field8
	 * @param field9
	 * @return  status OK 
	 */
	@Path("/insertLog")
	@GET
	@Timed
	public Response insertLog(@QueryParam("SVid") String SVId, @QueryParam("type") String type,
			@QueryParam("table") String table, @QueryParam("key") String key, @QueryParam("field0") String field0, @QueryParam("field1") String field1, @QueryParam("field2") String field2, @QueryParam("field3") String field3, @QueryParam("field4") String field4, @QueryParam("field5") String field5, @QueryParam("field6") String field6, @QueryParam("field7") String field7, @QueryParam("field8") String field8, @QueryParam("field9") String field9) {
		if (type.toLowerCase().equals("log")){
			if (table.equals("User")){
				return LogManager.getLogManager().insert(table, key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9);
			}
		}
		return Response.status(Status.BAD_REQUEST).build();
	}
	/**
	 * This method updates a tuple of type User in the log. 
	 * @param SVId The ID of a Storage View to be used to answer user's query.
	 * @param type The type of a Storage View to be used to answer user's query (Col,Row,AQP).
	 * @param table The table to be queried (Order/LineItem/user)
	 * @param key
	 * @param field0 		
	 * @param field1 
	 * @param field2 
	 * @param field3 
	 * @param field4
	 * @param field5
	 * @param field6
	 * @param field7
	 * @param field8
	 * @param field9
	 * @return  status OK 
	 */
	@Path("/updateLog")
	@GET
	@Timed
	public Response updateLog(@QueryParam("SVid") String SVId, @QueryParam("type") String type,
			@QueryParam("table") String table, @QueryParam("key") String key, @QueryParam("field0") String field0, @QueryParam("field1") String field1, @QueryParam("field2") String field2, @QueryParam("field3") String field3, @QueryParam("field4") String field4, @QueryParam("field5") String field5, @QueryParam("field6") String field6, @QueryParam("field7") String field7, @QueryParam("field8") String field8, @QueryParam("field9") String field9) {
		if (type.toLowerCase().equals("log")){
			if (table.equals("User")){
				return LogManager.getLogManager().update(table, key, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9);
			}
		}
		return Response.status(Status.BAD_REQUEST).build();
	}
	
	/**
	 * This method deletes a tuple of type User in the log. 
	 * @param SVId The ID of a Storage View to be used to answer user's query.
	 * @param type The type of a Storage View to be used to answer user's query (Col,Row,AQP).
	 * @param table The table to be queried (Order/LineItem/user)
	 * @param key
	 * @return  status OK 
	 */
	@Path("/deleteLog")
	@GET
	@Timed
	public Response deleteLog(@QueryParam("SVid") String SVId, @QueryParam("type") String type,
			@QueryParam("table") String table, @QueryParam("key") String key) {
		if (type.toLowerCase().equals("log")){
			if (table.equals("User")){
				return LogManager.getLogManager().delete(table, key);
			}
		}
		return Response.status(Status.BAD_REQUEST).build();
	}
	

}
