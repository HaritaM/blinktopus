package dbse.fopj.blinktopus.api.sv;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Abstract class to generalize Storage Views (Row,Col,AQP).
 * @author Pavlo Shevchenko (urmikl18), Harita Medimi and Gabriel Campero Durand
 *
 */
public class SV {
	
	private String id;
	private String type;
	private String table;
	private String attr;
	private double lower;
	private double higher;
	private int size;
	
	private long timeSV;

	
	/**
	 * Default constructor
	 */
	public SV() {
	}

	/**
	 * 
	 * @param id The ID this SV will be stored by.
	 * @param type The type of this SV.(Row,Col,AQP).
	 * @param table The table (Order or LineItem) the SV will be created on.
	 * @param attr The attribute (e.g. totalprice/extendedprice) the SV will be created on.
	 * @param lower	The lower boundary of a range query that invoked the creation of this SV.
	 * @param higher The higher boundary of a range query that invoked the creation of this SV.
	 */
	public SV(String id, String type, String table, String attr, double lower, double higher) {
		this.id = id;
		this.type = type;
		this.table=table;
		this.attr = attr;
		this.lower = lower;
		this.higher = higher;
	}
	
	/**
	 * 
	 * @return size
	 */
	public int getSize()
	{
		return size;
	}
	
	protected void setSize(int size)
	{
		this.size=size;
	}
	
	/**
	 * 
	 * @return timeSV
	 */
	public long getTime()
	{
		return timeSV;
	}
	
	protected void setTime(long timeSV)
	{
		this.timeSV=timeSV;
	}
	
	/**
	 * 
	 * @return id
	 */
	@JsonProperty
	public String getId()
	{
		return id;
	}
	
	/**
	 * 
	 * @return type
	 */
	@JsonProperty
	public String getType()
	{
		return type;
	}
	
	/**
	 * 
	 * @return table
	 */
	@JsonProperty
	public String getTable()
	{
		return table;
	}
	
	/**
	 * 
	 * @return attr
	 */
	@JsonProperty
	public String getAttr()
	{
		return attr;
	}
	
	/**
	 * 
	 * @return lower
	 */
	@JsonProperty
	public double getLower()
	{
		return lower;
	}
	
	/**
	 * 
	 * @return higher
	 */
	@JsonProperty
	public double getHigher()
	{
		return higher;
	}

}
