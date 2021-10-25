import org.apache.spark.sql.SparkSession;

public class CreateLookUpTable {
	Long card_id;
	String last_transaction;
	Integer postcode;
	Double UCL;
	String score;
	public void setcard_id(Long uCL) {
		card_id = uCL;
	}
	public Long getcard_id() {
		return card_id;
	}
	public String getLast_transaction() {
		return last_transaction;
	}
	public void setLast_transaction(String last_transaction) {
		this.last_transaction = last_transaction;
	}
	public Integer getPostcode() {
		return postcode;
	}
	public void setPostcode(Integer postcode) {
		this.postcode = postcode;
	}
	public Double getUCL() {
		return UCL;
	}
	public void setUCL(Double uCL) {
		UCL = uCL;
	}
	public String getScore() {
		return score;
	}
	public void setScore(String score) {
		this.score = score;
	}

	
	
	
}
