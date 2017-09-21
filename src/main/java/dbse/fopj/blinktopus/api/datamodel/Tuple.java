package dbse.fopj.blinktopus.api.datamodel;

/**
 * Abstract class to generalize tuples (Order/LineItem)
 * @author Pavlo Shevchenko (urmikl18)
 *
 */
public abstract class Tuple {
	protected String table = "";

	/**
	 * 
	 * @return Table's name (Order/LineItem).
	 */
	public String getTable() {
		return table;
	}
}
