package dbse.fopj.blinktopus.api.datamodel;

import org.hibernate.validator.constraints.Length;

import com.fasterxml.jackson.annotation.JsonProperty;

public class User extends Tuple{
	
	private boolean deleted;
	
	@Length(max = 60)
	private String key = null;
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

	private Integer versionId;
	private Long time;
	
	public Integer getVersionId() {
		return versionId;
	}

	public void setVersionId(Integer versionId) {
		this.versionId = versionId;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getField0() {
		return field0;
	}

	public void setField0(String field0) {
		this.field0 = field0;
	}

	public String getField1() {
		return field1;
	}

	public void setField1(String field1) {
		this.field1 = field1;
	}

	public String getField2() {
		return field2;
	}

	public void setField2(String field2) {
		this.field2 = field2;
	}

	public String getField3() {
		return field3;
	}

	public void setField3(String field3) {
		this.field3 = field3;
	}

	public String getField4() {
		return field4;
	}

	public void setField4(String field4) {
		this.field4 = field4;
	}

	public String getField5() {
		return field5;
	}

	public void setField5(String field5) {
		this.field5 = field5;
	}

	public String getField6() {
		return field6;
	}

	public void setField6(String field6) {
		this.field6 = field6;
	}

	public String getField7() {
		return field7;
	}

	public void setField7(String field7) {
		this.field7 = field7;
	}

	public String getField8() {
		return field8;
	}

	public void setField8(String field8) {
		this.field8 = field8;
	}

	public String getField9() {
		return field9;
	}

	public void setField9(String field9) {
		this.field9 = field9;
	}

	
	
	/**
	 * Default constructor.
	 */
	public User() {
		this.table = "User";
		this.key = null;
		this.deleted= false;
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
		this.table = "User";
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
		this.deleted = false;
	}

	public boolean isDeleted() {
		return deleted;
	}

	public void setDeleted(boolean deleted) {
		this.deleted = deleted;
	}
	
	public void delete (){
		this.deleted= true;
	}
}
