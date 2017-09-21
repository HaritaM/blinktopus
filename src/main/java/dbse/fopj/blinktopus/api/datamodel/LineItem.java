package dbse.fopj.blinktopus.api.datamodel;

import java.util.Date;

import org.hibernate.validator.constraints.Length;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Represents LineItem table for TPC-H benchmark dataset.
 * 
 * @author urmikl18
 *
 */
public class LineItem extends Tuple {
	private long orderKey;
	private long partKey;
	private long suppKey;
	private int lineNumber;
	private double quantity;
	private double extendedPrice;
	private double discount;
	private double tax;
	private char returnFlag;
	private char lineStatus;
	private Date shipDate;
	private Date commitDate;
	private Date receiptDate;
	@Length(max = 25)
	private String shipInstruct;
	@Length(max = 10)
	private String shipMode;
	@Length(max = 44)
	private String comment;

	/**
	 * Default constructor.
	 */
	public LineItem() {
	}

	/**
	 * 
	 * @param orderKey OrderKey
	 * @param partKey PartKey
	 * @param suppKey SuppKey
	 * @param lineNumber LineNumber
	 * @param quantity Quantity
	 * @param extendedPrice ExtendedPrice
	 * @param discount Discount
	 * @param tax Tax
	 * @param returnFlag ReturnFlag
	 * @param lineStatus LineStatus
	 * @param shipDate ShipDate
	 * @param commitDate CommitDate
	 * @param receiptDate ReceiptDate
	 * @param shipInstruct ShipInstruct
	 * @param shipMode ShipMode
	 * @param comment Comment
	 */
	public LineItem(long orderKey, long partKey, long suppKey, int lineNumber, double quantity, double extendedPrice,
			double discount, double tax, char returnFlag, char lineStatus, Date shipDate, Date commitDate,
			Date receiptDate, String shipInstruct, String shipMode, String comment) {
		this.table = "LineItem";
		this.orderKey = orderKey;
		this.partKey = partKey;
		this.suppKey = suppKey;
		this.lineNumber = lineNumber;
		this.quantity = quantity;
		this.extendedPrice = extendedPrice;
		this.discount = discount;
		this.tax = tax;
		this.returnFlag = returnFlag;
		this.lineStatus = lineStatus;
		this.shipDate = shipDate;
		this.commitDate = commitDate;
		this.receiptDate = receiptDate;
		this.shipInstruct = shipInstruct;
		this.shipMode = shipMode;
		this.comment = comment;
	}

	/**
	 * 
	 * @return OrderKey
	 */
	@JsonProperty
	public long getOrderKey() {
		return orderKey;
	}

	/**
	 * 
	 * @return partKey
	 */
	@JsonProperty
	public long getPartKey() {
		return partKey;
	}
	
	/**
	 * 
	 * @return suppKey
	 */
	@JsonProperty
	public long getSuppKey() {
		return suppKey;
	}
	
	/**
	 * 
	 * @return lineNumber
	 */
	@JsonProperty
	public int getLineNumber() {
		return lineNumber;
	}

	/**
	 * 
	 * @return quantity
	 */
	@JsonProperty
	public double getQuantity() {
		return quantity;
	}

	/**
	 * 
	 * @return extendedPrice
	 */
	@JsonProperty
	public double getExtendedPrice() {
		return extendedPrice;
	}

	/**
	 * 
	 * @return discount
	 */
	@JsonProperty
	public double getDiscount() {
		return discount;
	}

	/**
	 * 
	 * @return tax
	 */
	@JsonProperty
	public double getTax() {
		return tax;
	}

	/**
	 * 
	 * @return returnFlag
	 */
	@JsonProperty
	public char getReturnFlag() {
		return returnFlag;
	}

	/**
	 * 
	 * @return lineStatus
	 */
	@JsonProperty
	public char getLineStatus() {
		return lineStatus;
	}

	/**
	 * 
	 * @return shipDate
	 */
	@JsonProperty
	public Date getShipDate() {
		return shipDate;
	}

	/**
	 * 
	 * @return commitDate
	 */
	@JsonProperty
	public Date getCommitDate() {
		return commitDate;
	}

	/**
	 * 
	 * @return receiptDate
	 */
	@JsonProperty
	public Date getReceiptDate() {
		return receiptDate;
	}

	/**
	 * 
	 * @return shipInstruct
	 */
	@JsonProperty
	public String getShipInstruct() {
		return shipInstruct;
	}

	/**
	 * 
	 * @return shipMode
	 */
	@JsonProperty
	public String getShipMode() {
		return shipMode;
	}

	/**
	 * 
	 * @return comment
	 */
	@JsonProperty
	public String getComment() {
		return comment;
	}
}
