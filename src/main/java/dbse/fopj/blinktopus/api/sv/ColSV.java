package dbse.fopj.blinktopus.api.sv;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.fasterxml.jackson.annotation.JsonProperty;

import dbse.fopj.blinktopus.api.datamodel.LineItem;
import dbse.fopj.blinktopus.api.datamodel.Order;
import dbse.fopj.blinktopus.api.datamodel.Tuple;
import dbse.fopj.blinktopus.api.managers.LogManager;
import dbse.fopj.blinktopus.api.resultmodel.LogResult;
import dbse.fopj.blinktopus.api.resultmodel.Result;
import dbse.fopj.blinktopus.api.resultmodel.SVResult;
import dbse.fopj.blinktopus.resources.QueryProcessor;

/** Represents a column-oriented store. Created as a list of ColEntry's, which are double values and a position of this entry in a primary log.
 * @author Pavlo Shevchenko (urmikl18)
 *
 */
public class ColSV extends SV {
	class ColEntry {
		private double value;
		private int pos;

		public ColEntry() {
		}

		public ColEntry(double value, int pos) {
			this.value = value;
			this.pos = pos;
		}

		@JsonProperty
		public double getValue() {
			return value;
		}

		@JsonProperty
		public int getPos() {
			return pos;
		}
	}

	private List<ColEntry> colData;

	/**
	 * Default constructor.
	 */
	public ColSV() {
	}

	/**
	 * 
	 * @param id The ID this SV will be stored by.
	 * @param table The table (Order or LineItem) the SV will be created on.
	 * @param attr The attribute (e.g. totalprice/extendedprice) the SV will be created on.
	 * @param lower	The lower boundary of a range query that invoked the creation of this SV.
	 * @param higher The higher boundary of a range query that invoked the creation of this SV.
	 */
	public ColSV(String id, String table, String attr, double lower, double higher) {
		super(id, "Col", table, attr, lower, higher);
		List<Tuple> all = LogManager.getLogManager().getAllLog().getResultTuples();
		LogResult res = LogManager.getLogManager().scan(table, attr, lower, higher, "");
		List<Tuple> lr = res.getResultTuples();
		if (table.toLowerCase().equals("orders")) {
			switch (QueryProcessor.attrIndex.get(attr.toLowerCase())) {
			case 0:
				this.colData = IntStream.range(0, lr.size())
						.mapToObj(i -> new ColEntry(((Order) lr.get(i)).getTotalPrice(), all.indexOf(lr.get(i))))
						.collect(Collectors.toList());
				break;
			case 1:
				this.colData = IntStream.range(0, lr.size())
						.mapToObj(
								i -> new ColEntry(((Order) lr.get(i)).getOrderDate().getTime(), all.indexOf(lr.get(i))))
						.collect(Collectors.toList());
				break;
			default:
				this.colData = null;
				break;
			}
		} else {
			switch (QueryProcessor.attrIndex.get(attr.toLowerCase())) {
			case 0:
				this.colData = IntStream.range(0, lr.size())
						.mapToObj(i -> new ColEntry(((LineItem) lr.get(i)).getLineNumber(), all.indexOf(lr.get(i))))
						.collect(Collectors.toList());
				break;
			case 1:
				this.colData = IntStream.range(0, lr.size())
						.mapToObj(i -> new ColEntry(((LineItem) lr.get(i)).getQuantity(), all.indexOf(lr.get(i))))
						.collect(Collectors.toList());
				break;
			case 2:
				this.colData = IntStream.range(0, lr.size())
						.mapToObj(i -> new ColEntry(((LineItem) lr.get(i)).getExtendedPrice(), all.indexOf(lr.get(i))))
						.collect(Collectors.toList());
				break;
			case 3:
				this.colData = IntStream.range(0, lr.size())
						.mapToObj(i -> new ColEntry(((LineItem) lr.get(i)).getDiscount(), all.indexOf(lr.get(i))))
						.collect(Collectors.toList());
				break;
			case 4:
				this.colData = IntStream.range(0, lr.size())
						.mapToObj(i -> new ColEntry(((LineItem) lr.get(i)).getTax(), all.indexOf(lr.get(i))))
						.collect(Collectors.toList());
				break;
			case 5:
				this.colData = IntStream.range(0, lr.size()).mapToObj(
						i -> new ColEntry(((LineItem) lr.get(i)).getShipDate().getTime(), all.indexOf(lr.get(i))))
						.collect(Collectors.toList());
				break;
			case 6:
				this.colData = IntStream.range(0, lr.size()).mapToObj(
						i -> new ColEntry(((LineItem) lr.get(i)).getCommitDate().getTime(), all.indexOf(lr.get(i))))
						.collect(Collectors.toList());
				break;
			case 7:
				this.colData = IntStream.range(0, lr.size()).mapToObj(
						i -> new ColEntry(((LineItem) lr.get(i)).getReceiptDate().getTime(), all.indexOf(lr.get(i))))
						.collect(Collectors.toList());
				break;
			default:
				this.colData = null;
				break;
			}
		}
		this.setTime(res.getTimeLog());
		this.setSize(colData.size());
	}

	private ColSV(String id, String table, String attr, double lower, double higher, List<ColEntry> colData) {
		super(id, "Col", table, attr, lower, higher);
		this.colData = colData;
		this.setSize(colData.size());
	}

	/**
	 * 
	 * @return The current data stored in this Column SV.
	 */
	@JsonProperty
	public List<ColEntry> getColData() {
		return colData;
	}

	/**
	 * The method that returns relevant tuples and relevant information about the query to the user.
	 * @param table The table to be queried (Order/LineItem)
	 * @param attr The attribute to be queried on (e.g. totalprice/extendedprice)
	 * @param lower The lower boundary of a range query.
	 * @param higher The higher boundary of a range query.
	 * @return An instance of a class Result that contains information about the query (table, attr, lower, higher),
	 * information about SV (Id, Type (Col,Row,AQP)), information about result (tuples, size), and
	 * analytical information (time it took to retrieve the result, and error if necessary).
	 */
	public Result query(String table, String attr, double lower, double higher) {
		List<ColEntry> res = new ArrayList<ColEntry>();
		if (this.getTable().toLowerCase().equals(table.toLowerCase())
				&& this.getAttr().toLowerCase().equals(attr.toLowerCase()) && this.getLower() <= lower
				&& this.getHigher() >= higher) {
			long start = System.nanoTime();
			res = this.colData.stream().filter((ColEntry e) -> (e.getValue() >= lower && e.getValue() <= higher))
					.collect(Collectors.toList());
			long timeSV = System.nanoTime() - start;
			long timeLog = LogManager.getLogManager().getTime(table, attr, lower, higher, "");
			return new SVResult(this.getId() + "tmp", "Col", table, attr, lower, higher, timeLog, timeSV, res.size(), 0,
					0, "OK", new ColSV(this.getId() + "tmp", table, attr, lower, higher, res));
		} else {
			if (!this.getTable().toLowerCase().equals(table.toLowerCase()))
				return LogManager.getLogManager().scan(table, attr, lower, higher,
						"SV with Id: " + this.getId() + " is not for the table: " + table);
			else if (!this.getAttr().toLowerCase().equals(attr.toLowerCase()))
				return LogManager.getLogManager().scan(table, attr, lower, higher,
						"SV with Id: " + this.getId() + " is not for attribute: " + attr);
			else
				return LogManager.getLogManager().scan(table, attr, lower, higher, "Random error");
		}
	}

	/**
	 * 
	 * @param table The table to be queried (Order/LineItem)
	 * @param attr The attribute to be queried on (e.g. totalprice/extendedprice)
	 * @param lower The lower boundary of a range query.
	 * @param higher The higher boundary of a range query.
	 * @param distinct True, if only unique values should be counted, false otherwise.
	 * @param message Message for debug purposes.
	 * @return The number of (unique) values that satisfy given query.
	 */
	public long getCount(String table, String attr, double lower, double higher, boolean distinct, String message) {
		if (this.getTable().toLowerCase().equals(table.toLowerCase())
				&& this.getAttr().toLowerCase().equals(attr.toLowerCase()) && this.getLower() <= lower
				&& this.getHigher() >= higher) {
			if (!distinct) {
				return this.query(table, attr, lower, higher).getExactCount();
			} else {
				HashSet<Double> hs = new HashSet<>();
				for (ColEntry v : this.colData)
				{
					double val = v.getValue();
					if (val>=lower && val<=higher)
						hs.add(v.getValue());
						
				}
				return hs.size();
			}
		} else
			return LogManager.getLogManager().getCount(table, attr, lower, higher, distinct, message);
	}
}
