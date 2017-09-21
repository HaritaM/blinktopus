package dbse.fopj.blinktopus.api.resultmodel;

import com.fasterxml.jackson.annotation.JsonProperty;

import dbse.fopj.blinktopus.api.sv.*;

/**
 * Wrapper class to represent results from SVs.
 * @author Pavlo Shevchenko (urmikl18)
 *
 */
public class SVResult extends Result {

	private SV svResult;

	/**
	 * Default constructor.
	 */
	public SVResult() {
	}

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
	 * @param svResult Information about SV containing the result
	 */
	public SVResult(String SVid, String type, String table, String attr, double lower, double higher, long timeLog,
			long timeSV, long exactCount, long apprCount, double error, String message, SV svResult) {
		super(SVid, type, table, attr, lower, higher, timeLog, timeSV, exactCount, apprCount, error, message);
		this.svResult = svResult;
	}

	/**
	 * 
	 * @return svResult
	 */
	@JsonProperty
	public SV getSVResult() {
		return svResult;
	}
}
