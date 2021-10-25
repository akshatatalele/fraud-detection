import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class MainDriver {
	static String FulltableName = "card_trans_1";
	static String LookUpTableName = "lookuptable4";
	static String card_member = "card_member";
	static String member_score = "member_score";
	
	List<CreateLookUpTable> lookUpRecords = new ArrayList<>();
	Map<Long, Double> ucl = new HashMap<Long, Double>();
	Map<String, String> score = new HashMap<String, String>();
	Map<Long, String> transDt_postcode_member_id = new HashMap<Long, String>();
	SparkSession spark = SparkSession.builder().appName("DFToHive").enableHiveSupport().getOrCreate();

	
	public static void main(String[] args) {

		MainDriver driver = new MainDriver();
		driver.loadData(FulltableName);
		driver.loadDataFromRDS();
		driver.calculateUCL();
		driver.getLastTransaction_dtPostCode();
		driver.getScore();
		driver.populateLookUpTableList();
	}
	
	public void loadDataFromRDS(){
		System.out.println("############## creating hive table "+card_member+"#################");
		
		String query = "create external table if not exists "+card_member+"(`card_id` string,`member_id` string,`member_joining_dt` string,`card_purchase_dt` string,`country` string,`city` string)row format delimited fields terminated by ',' location '/user/ec2-user/Capstone_Card_Member'";
		spark.sql(query);

		query = "load data inpath '/user/ec2-user/Capstone/RDS_Card_Member' into table "+card_member;
		spark.sql(query);
		
		query = "SELECT * FROM " + card_member;
		Dataset<Row> sqlDF = spark.sql(query);
		sqlDF.show(20);
		
		System.out.println("############## creating hive table "+card_member+" Finished#################");
		
		System.out.println("############## creating hive table "+member_score+"#################");
		
		query = "create external table if not exists "+member_score+"(`member_id` string,`score` string)row format delimited fields terminated by ',' location '/user/ec2-user/Capstone_Member_score'";
		spark.sql(query);

		query = "load data inpath '/user/ec2-user/Capstone/RDS_Member_Score' into table "+member_score;
		spark.sql(query);
		
		query = "SELECT * FROM " + member_score;
		Dataset<Row> sqlDF1 = spark.sql(query);
		sqlDF1.show(20);
		
		System.out.println("############## creating hive table "+member_score+" Finished#################");
		
	}

	public void loadData(String FulltableName) {
		System.out.println("############## creating hive table #################");
		spark.sparkContext().setLogLevel("WARN");

		String path = "/user/ec2-user/Capstone/Files/card_transactions.csv";

		Dataset<Row> diamondDF = spark.read().format("csv").option("header", "true").option("inferSchema", "true")
				.load(path);

		diamondDF.printSchema();
//		diamondDF.show();
		
		diamondDF.write().format("orc").mode("overwrite").option("header", "true").saveAsTable(FulltableName);
		System.out.println("############## created hive table #################");

	}

	/*
	 * Query to find moving average and standard deviation for each card_id
	 * (Latest 10 transaction)
	 * 
	 * Returns Row: [6555788669879948,6776896.4,3056330.063605667] Card_id,
	 * moving average, standard deviation
	 */
	public void calculateUCL() {
		System.out.println("############## Calculating UCL values #################");
		spark.sparkContext().setLogLevel("WARN");

		String query1 = "SELECT rs.card_id,avg(rs.amount) as movingAverage, STDDEV(rs.amount) as standardDeviation FROM ( SELECT card_id,transaction_dt,amount, Rank() over (Partition BY card_id ORDER BY transaction_dt DESC ) AS Rank FROM "
				+ FulltableName + " WHERE status='GENUINE' ) rs WHERE Rank <= 10 GROUP BY rs.card_id";
		Dataset<Row> sqlDF = spark.sql(query1);
		sqlDF.show(20);

		List<Row> listOne = sqlDF.collectAsList();
		for (Row row2 : listOne) {
			ucl.put(row2.getLong(0), row2.getDouble(1) + 3 * row2.getDouble(2));
		}
		System.out.println(ucl.get(new Long("4009218272111551")));
		
		System.out.println("############## Calculating UCL values Finished #################");
	}

	/*
	 * Query to find Last transaction date and post code for each card_id
	 * 
	 * 
	 * Returns Row: [6555788669879948,6776896.4,3056330.063605667] 
	 * Card_id(long),transaction_dt(String), postcode(integer),member_id(Long)
	 */
	public void getLastTransaction_dtPostCode() {
		System.out.println("############## Calculating Last transaction date and post code values #################");
		spark.sparkContext().setLogLevel("WARN");

		String query1 = "SELECT rs.card_id,rs.transaction_dt, rs.postcode, rs.member_id FROM ( SELECT card_id,transaction_dt,postcode,member_id, Rank() over (Partition BY card_id ORDER BY transaction_dt DESC ) AS Rank FROM "
		+ FulltableName +" WHERE status='GENUINE' ) rs WHERE Rank <= 1";
		Dataset<Row> sqlDF = spark.sql(query1);
		sqlDF.show(20);
		
		List<Row> listOne = sqlDF.collectAsList();
		for (Row row2 : listOne) {
			transDt_postcode_member_id.put(row2.getLong(0), row2.getString(1) + "," + row2.getInt(2) + "," + row2.getLong(3));
		}
		System.out.println(transDt_postcode_member_id.get(new Long("4009218272111551")));
		System.out.println("############## Calculating Last transaction date and post code values Finished #################");
	
	}
	
	
	/*
	 * Query to find member id and score
	 * 
	 * member_id(Long),score(Long)
	 */
	public void getScore(){
		System.out.println("############## Calculating score values #################");
		spark.sparkContext().setLogLevel("WARN");

		String query1 = "SELECT member_id, score FROM "+member_score;
		Dataset<Row> sqlDF = spark.sql(query1);
		sqlDF.show(20);
		
		List<Row> listOne = sqlDF.collectAsList();
		for (Row row2 : listOne) {
			score.put(row2.getString(0), row2.getString(1));
		}
		System.out.println(score.get("341722035429601"));
		System.out.println("############## Calculating score values Finished #################");
	}
	
	public void populateLookUpTableList(){
		System.out.println("############## Populating look up table values #################");

		Set< Map.Entry<Long, Double> > st = ucl.entrySet();
		for (Map.Entry<Long, Double> me:st) {
			
			Long card_id = me.getKey();
			String transdt = transDt_postcode_member_id.get(card_id).split(",")[0];
			Integer postcode = Integer.parseInt(transDt_postcode_member_id.get(card_id).split(",")[1]);
//			String member_id = transDt_postcode_member_id.get(card_id).split(",")[2];
			
			CreateLookUpTable createLookUpTable = new CreateLookUpTable();
			createLookUpTable.setcard_id(card_id);
			createLookUpTable.setUCL(me.getValue());
			createLookUpTable.setLast_transaction(transdt);
			createLookUpTable.setPostcode(postcode);
			createLookUpTable.setScore(score.get(card_id.toString()));
			lookUpRecords.add(createLookUpTable);
		}
		
		String query = "create table if not exists "+LookUpTableName+"(`UCL` string, `card_id` string,`last_transaction` string,`postcode` string,`score` string) row format delimited fields terminated by ',' location '/user/ec2-user/Capstone_Lookup'";
		spark.sql(query);
		Dataset<Row> recordsDF = spark.createDataFrame(lookUpRecords, CreateLookUpTable.class);
		recordsDF.write().mode("overwrite").insertInto(LookUpTableName);
		
		System.out.println("############## Populating look up table values Finished#################");
		
		System.out.println("############## verify look up table values #################");
		String query1 = "SELECT * FROM " + LookUpTableName;
		Dataset<Row> sqlDF = spark.sql(query1);
		sqlDF.show(20);
		
		System.out.println("############## verify look up table values Finished#################");
	}

}
