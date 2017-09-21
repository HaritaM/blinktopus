package dbse.fopj.blinktopus.api.managers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import dbse.fopj.blinktopus.api.datamodel.LineItem;
import dbse.fopj.blinktopus.api.datamodel.Order;
import dbse.fopj.blinktopus.api.datamodel.Tuple;
import dbse.fopj.blinktopus.api.datamodel.User;
import dbse.fopj.blinktopus.api.resultmodel.LogResult;
import dbse.fopj.blinktopus.resources.QueryProcessor;

/** LogManager - class that operates the primary storage (primary log). Singleton class.
 * 
 * @author Pavlo Shevchenko (urmikl18)
 *
 */

public final class LogManager {
	private static final LogManager INSTANCE = new LogManager();
	private List<Tuple> dataLog = new ArrayList<Tuple>();
	ReentrantLock lock = new ReentrantLock();
	
	private LogManager() {
	}

	/**
	 * 
	 * @return The Singleton instance of LogManager
	 */
	public static LogManager getLogManager() {
		return INSTANCE;
	}

	/**
	 * Loads the tables into the primary storage. Load separately Order and LineItem. Then interleave the results.
	 * @param pathOrders Path to the Order table.
	 * @param pathLineItems Path to the LineItem table.
	 */
	public void loadData(String pathOrders, String pathLineItems) {
		this.dataLog.clear();
		ArrayList<Order> orders = new ArrayList<>();
		ArrayList<LineItem> lineitems = new ArrayList<>();

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		BufferedReader br = null;
		String l = "";
		String csvSplitBy = "\\|";
		
		long ordersCount = 0;
		long liCount=0;

		try {
			br = new BufferedReader(new FileReader(pathOrders));
			while ((l = br.readLine()) != null) {
				String[] line = l.split(csvSplitBy);
				if (line.length == 9)
					orders.add(new Order(Long.parseLong(line[0].trim()), Long.parseLong(line[1].trim()),
							line[2].trim().charAt(0), Double.parseDouble(line[3].trim()), format.parse(line[4].trim()),
							line[5].trim(), line[6].trim(), Integer.parseInt(line[7].trim()), line[8].trim()));
				++ordersCount;
				if (ordersCount%120000==0)
					System.out.println(ordersCount+" orders were loaded");
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		try {
			br = new BufferedReader(new FileReader(pathLineItems));
			while ((l = br.readLine()) != null) {
				String[] line = l.split(csvSplitBy);
				if (line.length == 16)
					lineitems.add(new LineItem(Long.parseLong(line[0].trim()), Long.parseLong(line[1].trim()),
							Long.parseLong(line[2].trim()), Integer.parseInt(line[3].trim()),
							Double.parseDouble(line[4].trim()), Double.parseDouble(line[5].trim()),
							Double.parseDouble(line[6].trim()), Double.parseDouble(line[7].trim()),
							line[8].trim().charAt(0), line[9].trim().charAt(0), format.parse(line[10].trim()),
							format.parse(line[11].trim()), format.parse(line[12].trim()), line[13].trim(),
							line[14].trim(), line[15].trim()));
				++liCount;
				if (liCount%550000==0)
				{
					System.out.println(liCount+" lineitems were loaded");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

		interleave(orders, lineitems);
		orders.clear();
		lineitems.clear();
		orders = null;
		lineitems = null;
	}

	private void interleave(ArrayList<Order> orders, ArrayList<LineItem> lineitems) {
		Iterator<Order> it1 = orders.iterator();
		Iterator<LineItem> it2 = lineitems.iterator();
		int expectedSize = orders.size() + lineitems.size();
		int coin = 0;

		while (this.dataLog.size() != expectedSize) {
			coin = (Math.random() < 0.5) ? 0 : 1;
			if (coin == 0) {
				if (it1.hasNext())
					this.dataLog.add(it1.next());
				else
					this.dataLog.add(it2.next());
			} else {
				if (it2.hasNext())
					this.dataLog.add(it2.next());
				else
					this.dataLog.add(it1.next());
			}
		}
	}

	/**
	 * 
	 * @return All entries currently stored in a log.
	 */
	public LogResult getAllLog() {
		long start = System.nanoTime();
		return new LogResult("Primary", "Log", "", "", 0, 0, System.nanoTime() - start, 0, this.dataLog.size(), 0, 0,
				"OK", this.dataLog);
	}

	/**
	 * 
	 * The method that returns relevant tuples and relevant information about the query to the user.
	 * @param table The table to be queried (Order/LineItem)
	 * @param attr The attribute to be queried on (e.g. totalprice/extendedprice)
	 * @param lower The lower boundary of a range query.
	 * @param higher The higher boundary of a range query.
	 * @param message Debug message.
	 * @return An instance of a class Result that contains information about the query (table, attr, lower, higher),
	 * information about SV (Id, Type (Col,Row,AQP)), information about result (tuples, size), and
	 * analytical information (time it took to retrieve the result, and error if necessary).
	 */
	public LogResult scan(String table, String attr, double lower, double higher, String message) {
		List<Tuple> res = new ArrayList<Tuple>();

		long start = System.nanoTime();
		if (table.toLowerCase().equals("orders")) {
			switch (QueryProcessor.attrIndex.get(attr.toLowerCase())) {
			case 0:
				res = this.dataLog.stream()
						.filter((Tuple e) -> (e.getTable().toLowerCase().equals("order")
								&& ((Order) e).getTotalPrice() >= lower && ((Order) e).getTotalPrice() <= higher))
						.collect(Collectors.toList());
				break;
			case 1:
				res = this.dataLog.stream()
						.filter((Tuple e) -> (e.getTable().toLowerCase().equals("order")
								&& ((Order) e).getOrderDate().getTime() >= lower
								&& ((Order) e).getOrderDate().getTime() <= higher))
						.collect(Collectors.toList());
				break;
			default:
				res = null;
				break;
			}
		} else {
			switch (QueryProcessor.attrIndex.get(attr.toLowerCase())) {
			case 0:
				res = this.dataLog.stream()
						.filter((Tuple e) -> (e.getTable().toLowerCase().equals("lineitem")
								&& ((LineItem) e).getLineNumber() >= lower && ((LineItem) e).getLineNumber() <= higher))
						.collect(Collectors.toList());
				break;
			case 1:
				res = this.dataLog.stream()
						.filter((Tuple e) -> (e.getTable().toLowerCase().equals("lineitem")
								&& ((LineItem) e).getQuantity() >= lower && ((LineItem) e).getQuantity() <= higher))
						.collect(Collectors.toList());
				break;
			case 2:
				res = this.dataLog.stream()
						.filter((Tuple e) -> (e.getTable().toLowerCase().equals("lineitem")
								&& ((LineItem) e).getExtendedPrice() >= lower
								&& ((LineItem) e).getExtendedPrice() <= higher))
						.collect(Collectors.toList());
				break;
			case 3:
				res = this.dataLog.stream()
						.filter((Tuple e) -> (e.getTable().toLowerCase().equals("lineitem")
								&& ((LineItem) e).getDiscount() >= lower && ((LineItem) e).getDiscount() <= higher))
						.collect(Collectors.toList());
				break;
			case 4:
				res = this.dataLog.stream()
						.filter((Tuple e) -> (e.getTable().toLowerCase().equals("lineitem")
								&& ((LineItem) e).getTax() >= lower && ((LineItem) e).getTax() <= higher))
						.collect(Collectors.toList());
				break;
			case 5:
				res = this.dataLog.stream()
						.filter((Tuple e) -> (e.getTable().toLowerCase().equals("lineitem")
								&& ((LineItem) e).getShipDate().getTime() >= lower
								&& ((LineItem) e).getShipDate().getTime() <= higher))
						.collect(Collectors.toList());
				break;
			case 6:
				res = this.dataLog.stream()
						.filter((Tuple e) -> (e.getTable().toLowerCase().equals("lineitem")
								&& ((LineItem) e).getCommitDate().getTime() >= lower
								&& ((LineItem) e).getCommitDate().getTime() <= higher))
						.collect(Collectors.toList());
				break;
			case 7:
				res = this.dataLog.stream()
						.filter((Tuple e) -> (e.getTable().toLowerCase().equals("lineitem")
								&& ((LineItem) e).getReceiptDate().getTime() >= lower
								&& ((LineItem) e).getReceiptDate().getTime() <= higher))
						.collect(Collectors.toList());
				break;
			default:
				res = null;
				break;
			}
		}
		return new LogResult("Primary", "Log", table, attr, lower, higher, System.nanoTime() - start, 0, res.size(), 0,
				0, message, res);
	}

	/**
	 * 
	 * @param table The table to be queried (Order/LineItem)
	 * @param attr The attribute to be queried on (e.g. totalprice/extendedprice)
	 * @param lower The lower boundary of a range query.
	 * @param higher The higher boundary of a range query.
	 * @param message Message for debug purposes.
	 * @return The time it takes to run a certain query over the log. Used for evaluation purposes.
	 */
	public long getTime(String table, String attr, double lower, double higher, String message) {
		LogResult r = this.scan(table, attr, lower, higher, message);
		return r.getTimeLog();
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
		if (!distinct) {
			LogResult r = this.scan(table, attr, lower, higher, message);
			return r.getExactCount();
		} else {
			List<Tuple> r = this.scan(table, attr, lower, higher, message).getResultTuples();
			HashSet<Double> hs = new HashSet<>();
			for (Tuple t : r) {
				if (table.toLowerCase().equals("orders")) {
					switch (QueryProcessor.attrIndex.get(attr.toLowerCase())) {
					case 0:
						hs.add(((Order) t).getTotalPrice());
						break;
					case 1:
						hs.add((double) ((Order) t).getOrderDate().getTime());
						break;
					default:
						break;
					}
				} else {
					switch (QueryProcessor.attrIndex.get(attr.toLowerCase())) {
					case 0:
						hs.add((double) ((LineItem) t).getLineNumber());
						break;
					case 1:
						hs.add((double) ((LineItem) t).getQuantity());
						break;
					case 2:
						hs.add((double) ((LineItem) t).getExtendedPrice());
						break;
					case 3:
						hs.add((double) ((LineItem) t).getDiscount());
						break;
					case 4:
						hs.add((double) ((LineItem) t).getTax());
						break;
					case 5:
						hs.add((double) ((LineItem) t).getShipDate().getTime());
						break;
					case 6:
						hs.add((double) ((LineItem) t).getCommitDate().getTime());
						break;
					case 7:
						hs.add((double) ((LineItem) t).getReceiptDate().getTime());
						break;
					default:
						break;
					}
				}
			}
			return hs.size();
		}
	}
	
	public void insert(String table, String key, String field0, String field1,String field2, String field3,String field4, String field5,String field6,String field7,String field8,String field9 ){
		User user = new User (key,field0, field1, field2,field3,  field4, field5, field6,  field7, field8,  field9);
		lock.lock();
		try{
			dataLog.add(user);
		}
		finally {
	        lock.unlock();
	    }
	}
	
	public User read(String table, String key){
		User result=null;
		lock.lock();
		try{
			List<User> users = dataLog.stream().filter(it->it.getTable()=="User").map(it->(User)it).filter(it->it.getKey()==key).collect(Collectors.toList());
			if (!users.isEmpty()){
				result = users.get(0);
				System.out.println("We found a user");
			}
			else{
				System.out.println("So sorry, we found no user :(");
			}
		}
		finally {
	        lock.unlock();
	    }
		return result;
	}
}
