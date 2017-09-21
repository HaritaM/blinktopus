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

/**
 * Represents a row-oriented store. A list of RowEntry, that contains id of a tuple, tuple itself and its position in the primary log.
 * @author Pavlo Shevchenko (urmikl18)
 *
 */
public class RowSV extends SV {
	class RowEntry {
		private int id;
		private Tuple value;
		private int pos;

		public RowEntry() {
		}

		public RowEntry(int id, Tuple value, int pos) {
			this.id = id;
			this.value = value;
			this.pos = pos;
		}

		@JsonProperty
		public int getId() {
			return id;
		}

		@JsonProperty
		public Tuple getValue() {
			return value;
		}

		@JsonProperty
		public int getPosition() {
			return pos;
		}
	}

	private List<RowEntry> rowData;

	/**
	 * Default constructor.
	 */
	public RowSV() {
	}

	/**
	 * 
	 * @param id The ID this SV will be stored by.
	 * @param table The table (Order or LineItem) the SV will be created on.
	 * @param attr The attribute (e.g. totalprice/extendedprice) the SV will be created on.
	 * @param lower	The lower boundary of a range query that invoked the creation of this SV.
	 * @param higher The higher boundary of a range query that invoked the creation of this SV.
	 */
	public RowSV(String id, String table, String attr, double lower, double higher) {
		super(id, "Row", table, attr, lower, higher);
		List<Tuple> all = LogManager.getLogManager().getAllLog().getResultTuples();
		LogResult res = LogManager.getLogManager().scan(table, attr, lower, higher, "");
		List<Tuple> lr = res.getResultTuples();
		this.setTime(res.getTimeLog());
		this.rowData = IntStream.range(0, lr.size())
				.mapToObj(i -> new RowEntry(i + 1, lr.get(i), all.indexOf(lr.get(i)))).collect(Collectors.toList());
		this.setSize(rowData.size());
	}

	private RowSV(String id, String table, String attr, double lower, double higher, List<RowEntry> rowData) {
		super(id, "Row", table, attr, lower, higher);
		this.rowData = rowData;
		this.setSize(rowData.size());
	}

	/**
	 * 
	 * @return rowData
	 */
	@JsonProperty
	public List<RowEntry> getRowData() {
		return rowData;
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
		long start = 0;
		long timeSV = 0;
		if (this.getTable().toLowerCase().equals(table.toLowerCase())
				&& this.getAttr().toLowerCase().equals(attr.toLowerCase()) && this.getLower() <= lower
				&& this.getHigher() >= higher) {
			List<RowEntry> res = new ArrayList<RowEntry>();
			if (table.toLowerCase().equals("orders")) {
				switch (QueryProcessor.attrIndex.get(attr.toLowerCase())) {
				case 0:
					start = System.nanoTime();
					res = this.rowData.stream().filter((RowEntry e) -> ((Order) e.getValue()).getTotalPrice() >= lower
							&& ((Order) e.getValue()).getTotalPrice() <= higher).collect(Collectors.toList());
					timeSV = System.nanoTime() - start;
					break;
				case 1:
					start = System.nanoTime();
					res = this.rowData.stream()
							.filter((RowEntry e) -> ((Order) e.getValue()).getOrderDate().getTime() >= lower
									&& ((Order) e.getValue()).getOrderDate().getTime() <= higher)
							.collect(Collectors.toList());
					timeSV = System.nanoTime() - start;
					break;
				default:
					res = null;
					break;
				}
			} else {
				switch (QueryProcessor.attrIndex.get(attr.toLowerCase())) {
				case 0:
					start = System.nanoTime();
					res = this.rowData.stream()
							.filter((RowEntry e) -> ((LineItem) e.getValue()).getLineNumber() >= lower
									&& ((LineItem) e.getValue()).getLineNumber() <= higher)
							.collect(Collectors.toList());
					timeSV = System.nanoTime() - start;
					break;
				case 1:
					start = System.nanoTime();
					res = this.rowData.stream().filter((RowEntry e) -> ((LineItem) e.getValue()).getQuantity() >= lower
							&& ((LineItem) e.getValue()).getQuantity() <= higher).collect(Collectors.toList());
					timeSV = System.nanoTime() - start;
					break;
				case 2:
					start = System.nanoTime();
					res = this.rowData.stream()
							.filter((RowEntry e) -> ((LineItem) e.getValue()).getExtendedPrice() >= lower
									&& ((LineItem) e.getValue()).getExtendedPrice() <= higher)
							.collect(Collectors.toList());
					timeSV = System.nanoTime() - start;
					break;
				case 3:
					start = System.nanoTime();
					res = this.rowData.stream().filter((RowEntry e) -> ((LineItem) e.getValue()).getDiscount() >= lower
							&& ((LineItem) e.getValue()).getDiscount() <= higher).collect(Collectors.toList());
					timeSV = System.nanoTime() - start;
					break;
				case 4:
					start = System.nanoTime();
					res = this.rowData.stream().filter((RowEntry e) -> ((LineItem) e.getValue()).getTax() >= lower
							&& ((LineItem) e.getValue()).getTax() <= higher).collect(Collectors.toList());
					timeSV = System.nanoTime() - start;
					break;
				case 5:
					start = System.nanoTime();
					res = this.rowData.stream()
							.filter((RowEntry e) -> ((LineItem) e.getValue()).getShipDate().getTime() >= lower
									&& ((LineItem) e.getValue()).getShipDate().getTime() <= higher)
							.collect(Collectors.toList());
					timeSV = System.nanoTime() - start;
					break;
				case 6:
					start = System.nanoTime();
					res = this.rowData.stream()
							.filter((RowEntry e) -> ((LineItem) e.getValue()).getCommitDate().getTime() >= lower
									&& ((LineItem) e.getValue()).getCommitDate().getTime() <= higher)
							.collect(Collectors.toList());
					timeSV = System.nanoTime() - start;
					break;
				case 7:
					start = System.nanoTime();
					res = this.rowData.stream()
							.filter((RowEntry e) -> ((LineItem) e.getValue()).getReceiptDate().getTime() >= lower
									&& ((LineItem) e.getValue()).getReceiptDate().getTime() <= higher)
							.collect(Collectors.toList());
					timeSV = System.nanoTime() - start;
					break;
				default:
					res = null;
					break;
				}
			}
			long timeLog = LogManager.getLogManager().getTime(table, attr, lower, higher, "");
			return new SVResult(this.getId() + "tmp", "Row", table, attr, lower, higher, timeLog, timeSV, res.size(), 0,
					0, "OK", new RowSV(this.getId() + "tmp", table, attr, lower, higher, res));

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
				double val = 0.0;
				HashSet<Double> hs = new HashSet<>();
				for (RowEntry v : this.rowData) {
					Tuple t = v.getValue();
					if (table.toLowerCase().equals("orders")) {
						switch (QueryProcessor.attrIndex.get(attr.toLowerCase())) {
						case 0:
							val = ((Order) t).getTotalPrice();
							if (val>=lower && val <=higher) 
								hs.add(val);
							break;
						case 1:
							val = (double) ((Order) t).getOrderDate().getTime();
							if (val>=lower && val <=higher) 
								hs.add(val);
							break;
						default:
							break;
						}
					} else {
						switch (QueryProcessor.attrIndex.get(attr.toLowerCase())) {
						case 0:
							val = (double) ((LineItem) t).getLineNumber();
							if (val>=lower && val <=higher) 
								hs.add(val);
							break;
						case 1:
							val = (double) ((LineItem) t).getQuantity();
							if (val>=lower && val <=higher) 
								hs.add(val);
							break;
						case 2:
							val = (double) ((LineItem) t).getExtendedPrice();
							if (val>=lower && val <=higher) 
								hs.add(val);
							break;
						case 3:
							val = (double) ((LineItem) t).getDiscount();
							if (val>=lower && val <=higher) 
								hs.add(val);
							break;
						case 4:
							val = (double) ((LineItem) t).getTax();
							if (val>=lower && val <=higher) 
								hs.add(val);
							break;
						case 5:
							val = (double) ((LineItem) t).getShipDate().getTime();
							if (val>=lower && val <=higher) 
								hs.add(val);
							break;
						case 6:
							val = (double) ((LineItem) t).getCommitDate().getTime();
							if (val>=lower && val <=higher) 
								hs.add(val);
							break;
						case 7:
							val = (double) ((LineItem) t).getReceiptDate().getTime();
							if (val>=lower && val <=higher) 
								hs.add(val);
							break;
						default:
							break;
						}
					}
				}
				return hs.size();
			}
		} else
			return LogManager.getLogManager().getCount(table, attr, lower, higher, distinct, message);
	}

}
