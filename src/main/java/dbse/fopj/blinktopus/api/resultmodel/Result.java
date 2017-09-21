package dbse.fopj.blinktopus.api.resultmodel;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Abstract class to generalize results (LogResult, SVManagerResult, SVResult).
 * @author Pavlo Shevchenko (urmikl18)
 *
 */
public abstract class Result {
	private String SVid;
	
	private String type;
	private String table;
	private String attr;
	private double lower;
	private double higher;
	
	private long timeLog;
	private long timeSV;
	private long exactCount;
	private long apprCount;
	private double error;
	
	
	private String message;

	/**
	 * Default constructor.
	 */
	public Result() {

	}

	/**
	 * 
/**
	 * 
	 * @param SVid SV's ID used to answer the given query.
	 * @param type SV's type used to answer the given query.
	 * @param table Table that was queried (Order/LineItem).
	 * @param attr Attribute that was queried (e.g. totalprice/extendedprice)
	 * @param lower The lower boundary of a range query.
	 * @param higher The higher boundary of a range query.
	 * @param timeLog The time it takes to retrieve data from the primary log.
	 * @param timeSV The time it takes to retrieve data from the Storage View.
	 * @param exactCount The exact number of (unique) values in the given range.
	 * @param apprCount The approximate number of (unique) values in the given range.
	 * @param error The absolute error of approximation.
	 * @param message Debug message.
	 */
	public Result(String SVid, String type, String table, String attr, double lower, double higher, long timeLog,
			long timeSV, long exactCount, long apprCount, double error, String message) {
		this.SVid = SVid;
		this.type = type;
		this.table = table;
		this.attr = attr;
		this.lower = lower;
		this.higher = higher;
		this.timeLog = timeLog;
		this.timeSV = timeSV;
		this.exactCount=exactCount;
		this.apprCount=apprCount;
		this.error = error;
		this.message = message;
	}

	/**
	 * 
	 * @return SVid
	 */
	@JsonProperty
	public String getSVid() {
		return SVid;
	}

	/**
	 * 
	 * @return type
	 */
	@JsonProperty
	public String getType() {
		return type;
	}

	/**
	 * 
	 * @return table
	 */
	@JsonProperty
	public String getTable() {
		return table;
	}

	/**
	 * 
	 * @return attr
	 */
	@JsonProperty
	public String getAttr() {
		return attr;
	}

	/**
	 * 
	 * @return lower
	 */
	@JsonProperty
	public double getLower() {
		return lower;
	}

	/**
	 * 
	 * @return higher
	 */
	@JsonProperty
	public double getHigher() {
		return higher;
	}

	/**
	 * 
	 * @return timeLog
	 */
	@JsonProperty
	public long getTimeLog() {
		return timeLog;
	}

	/**
	 * 
	 * @return timeSV
	 */
	@JsonProperty
	public long getTimeSV() {
		return timeSV;
	}

	/**
	 * 
	 * @return exactCount
	 */
	@JsonProperty
	public long getExactCount() {
		return exactCount;
	}
	
	/**
	 * 
	 * @return apprCount
	 */
	@JsonProperty
	public long getApprCount() {
		return apprCount;
	}

	/**
	 * 
	 * @return error
	 */
	@JsonProperty
	public double getError() {
		return error;
	}

	/**
	 * 
	 * @return message
	 */
	@JsonProperty
	public String getMessage() {
		return message;
	}
}
