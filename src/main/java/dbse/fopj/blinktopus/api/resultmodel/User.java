package dbse.fopj.blinktopus.api.resultmodel;

import org.hibernate.validator.constraints.Length;

import com.fasterxml.jackson.annotation.JsonProperty;

public class User {
	@Length(max = 60)
	private String key;
	@JsonProperty
	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}
	@JsonProperty
	public String getField0() {
		return field0;
	}

	public void setField0(String field0) {
		this.field0 = field0;
	}

	@JsonProperty
	public String getField1() {
		return field1;
	}

	public void setField1(String field1) {
		this.field1 = field1;
	}
	
	@JsonProperty
	public String getField2() {
		return field2;
	}

	public void setField2(String field2) {
		this.field2 = field2;
	}

	@JsonProperty
	public String getField3() {
		return field3;
	}

	public void setField3(String field3) {
		this.field3 = field3;
	}

	@JsonProperty
	public String getField4() {
		return field4;
	}

	public void setField4(String field4) {
		this.field4 = field4;
	}

	@JsonProperty
	public String getField5() {
		return field5;
	}

	public void setField5(String field5) {
		this.field5 = field5;
	}

	@JsonProperty
	public String getField6() {
		return field6;
	}

	public void setField6(String field6) {
		this.field6 = field6;
	}

	@JsonProperty
	public String getField7() {
		return field7;
	}

	public void setField7(String field7) {
		this.field7 = field7;
	}

	@JsonProperty
	public String getField8() {
		return field8;
	}

	public void setField8(String field8) {
		this.field8 = field8;
	}

	@JsonProperty
	public String getField9() {
		return field9;
	}

	public void setField9(String field9) {
		this.field9 = field9;
	}

	@Length(max = 60)
	private String field0;
	@Length(max = 60)
	private String field1;
	@Length(max = 60)
	private String field2;
	@Length(max = 60)
	private String field3;
	@Length(max = 60)
	private String field4;
	@Length(max = 60)
	private String field5;
	@Length(max = 60)
	private String field6;
	@Length(max = 60)
	private String field7;
	@Length(max = 60)
	private String field8;
	@Length(max = 60)
	private String field9;
	
	/**
	 * Default constructor.
	 */
	public User() {
	}
	
	/**
	 * 
	 * @param key key
	 * @param field0 field0
	 * @param field1 field1
	 * @param field2 field2
	 * @param field3 field3
	 * @param field4 field4
	 * @param field5 field5
	 * @param field6 field6
	 * @param field7 field7
	 * @param field8 field8
	 * @param field9 field9
	 * 	 */
	public User(String key, String field0, String field1, String field2, String field3, String field4, String field5, String field6, String field7, String field8, String field9) {
		this.key = key;
		this.field0 = field0;
		this.field1 = field1;
		this.field2 = field2;
		this.field3 = field3;
		this.field4 = field4;
		this.field5 = field5;
		this.field6 = field6;
		this.field7 = field7;
		this.field8 = field8;
		this.field9 = field9;
	}
	
	public User (dbse.fopj.blinktopus.api.datamodel.User user){
		this.key= user.getKey();
		this.field0 =user.getField0();
		this.field1 = user.getField1();
		this.field2 = user.getField2();
		this.field3 = user.getField3();
		this.field4 = user.getField4();
		this.field5 = user.getField5();
		this.field6 = user.getField6();
		this.field7 = user.getField7();
		this.field8 = user.getField8();
		this.field9 = user.getField9();
	}

}
