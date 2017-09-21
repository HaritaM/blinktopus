package dbse.fopj.blinktopus.api.resultmodel;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import dbse.fopj.blinktopus.api.sv.SV;

/**
 * Wrapper class to represent SVManager.
 * @author Pavlo Shevchenko (urmikl18)
 *
 */
public class SVManagerResult extends Result {

	private List<SV> allSV;

	/**
	 * Default constructor.
	 */
	public SVManagerResult() {
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
	 * @param allSV The list of currently stored SVs.
	 */
	public SVManagerResult(String SVid, String type, String table, String attr, double lower, double higher,
			long timeLog, long timeSV, long exactCount, long apprCount, double error, String message, List<SV> allSV) {
		super(SVid, type, table, attr, lower, higher, timeLog, timeSV, exactCount, apprCount, error, message);
		this.allSV = allSV;
	}

	/**
	 * 
	 * @return allSV
	 */
	@JsonProperty
	public List<SV> getAllSV() {
		return allSV;
	}

}
