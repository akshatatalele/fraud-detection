import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TransactionDataLoad {
	SparkSession spark = SparkSession.builder().appName("DFToHive").enableHiveSupport().getOrCreate();
	public void loadData(String FulltableName){
		
		spark.sparkContext().setLogLevel("WARN");

		String path = "/user/ec2-user/Capstone/Files/card_transactions.csv";
		

		Dataset<Row> diamondDF = spark.read().format("csv").option("header", "true").option("inferSchema", "true")
				.load(path);

		diamondDF.printSchema();
//		diamondDF.show();
		System.out.println("############## creating hive table #################");
		diamondDF.write().format("orc").mode("overwrite").option("header", "true").saveAsTable(FulltableName);
		System.out.println("############## created hive table #################");

	}
}
