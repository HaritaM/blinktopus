package dbse.fopj.blinktopus.api.managers;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import dbse.fopj.blinktopus.api.resultmodel.Result;
import dbse.fopj.blinktopus.api.resultmodel.SVManagerResult;
import dbse.fopj.blinktopus.api.resultmodel.SVResult;
import dbse.fopj.blinktopus.api.sv.*;

/** SVManager - stores, organizes Storage Views and redirects the user's query to specified SV. Singleton class.
 * 
 * @author Pavlo Shevchenko (urmikl18)
 *
 */
public final class SVManager {
	private List<SV> allSV = new ArrayList<SV>();
	private static final SVManager INSTANCE = new SVManager();
	private int idSV = 1;

	private SVManager() {
	}

	/**
	 * 
	 * @return The Singleton instance of class SVManager.
	 */
	public static SVManager getSVManager() {
		return INSTANCE;
	}

	/**
	 * 
	 * @return List of currently stored SVs
	 */
	public SVManagerResult getAllSV() {
		long start = System.nanoTime();
		return new SVManagerResult("SV", "Manager", "", "", 0, 0, 0, System.nanoTime() - start, allSV.size(),0, 0, "OK",
				allSV);
	}
	
	/**
	 * Deletes all SVs, that are currently stored.
	 */
	public void clear()
	{
		SV aqp = this.allSV.get(0);
		this.allSV.clear();
		this.allSV.add(aqp);
		this.idSV=1;
	}

	/**
	 * 
	 * @param SVId ID of a Storage View to be maintained.
	 * @param type Type of SV (Col, Row, AQP).
	 * @param table Table that SV should be created on.(Order/LineItem)
	 * @param attr Attribute that SV should be created on.(e.g. totalprice/extendedprice)
	 * @param lower The lower boundary of a range query.
	 * @param higher The higher boundary of a range query.
	 * @param createSV True, if new SV of given type should be created, false, if already existing SV with SVId should be used.
	 * @param distinct Used for count queries, using AQP.
	 * @return An instance of a class Result that contains information about the query (table, attr, lower, higher),
	 * information about SV (Id, Type (Col,Row,AQP)), information about result (tuples, size), and
	 * analytical information (time it took to retrieve the result, and error if necessary).
	 */
	public Result maintain(String SVId, String type, String table, String attr, double lower, double higher,
			boolean createSV, boolean distinct) {
		if (createSV) {
			if (type.toLowerCase().equals("row")) {
				String rowId = "Row" + (idSV++);
				RowSV res = new RowSV(rowId, table, attr, lower, higher);
				this.allSV.add(res);
				long timeSV = res.getTime();
				return new SVResult(rowId, type, table, attr, lower, higher, 0, timeSV, res.getSize(),0, 0, "OK",
						res);
			} else if (type.toLowerCase().equals("col")) {
				String colId = "Col" + (idSV++);
				ColSV res = new ColSV(colId, table, attr, lower, higher);
				this.allSV.add(res);
				long timeSV = res.getTime();
				return new SVResult(colId, type, table, attr, lower, higher, 0, timeSV, res.getSize(),0, 0, "OK",
						res);
			} else {
				String aqpId = "AQP";
				AqpSV res = new AqpSV();
				this.allSV.add(res);
								
				long exactCount = LogManager.getLogManager().getCount(table, attr, lower, higher, distinct, "Exact count");
				long apprCount = res.query(table, attr, lower, higher, distinct);
				return new SVResult(aqpId, type, table, attr, lower, higher, 0, res.getTime(),
						exactCount, apprCount, apprCount - exactCount, "OK", res);			
			}
		} else {
			SV sv = new SV();
			try {
				sv = this.allSV.stream().filter((SV r) -> r.getId().toLowerCase().equals(SVId.toLowerCase()))
						.collect(Collectors.toList()).get(0);

			} catch (IndexOutOfBoundsException e) {
				return LogManager.getLogManager().scan(table, attr, lower, higher, "SV with Id: "+SVId+" doesn't exist");
			}
			if (sv == null)
				return LogManager.getLogManager().scan(table, attr, lower, higher, "SV with Id: "+SVId+" doesn't exist");
			if (!sv.getType().toLowerCase().equals(type.toLowerCase()))
				return LogManager.getLogManager().scan(table, attr, lower, higher, "SV with Id: "+SVId+" doesn't have type: "+type);

			if (sv.getType().toLowerCase().equals("row")) {
				RowSV rowSV = (RowSV) sv;
				return rowSV.query(table, attr, lower, higher);
			} else if (sv.getType().toLowerCase().equals("col")) {
				ColSV colSV = (ColSV) sv;
				return colSV.query(table, attr, lower, higher);
			} else {
				AqpSV aqpSV = (AqpSV) sv;
				long startTime = System.nanoTime();
				long exactCount = LogManager.getLogManager().getCount(table, attr, lower, higher, distinct, "Exact count");
				long logTime = System.nanoTime()-startTime;
				long apprCount = aqpSV.query(table, attr, lower, higher, distinct);
				return new SVResult(SVId, type, table, attr, lower, higher, logTime, aqpSV.getTime(),
						exactCount, apprCount, apprCount - exactCount, "OK", aqpSV);
			}
		}
	}
}
