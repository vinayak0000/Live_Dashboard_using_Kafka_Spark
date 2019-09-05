package com.springKafka.liveDashboard.services;


import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.stereotype.Service;

import com.kafka.producer.NetworkSignal;

@Service
public class SparkServiceImpl implements SparkService {


	public List<NetworkSignal> readFromFile() {

		SparkConf conf = new SparkConf().set("spark.sql.parquet.mergeSchema", "false")
				.set("parquet.enable.summary-metadata", "false");
			      
		SparkSession spark = SparkSession	
				  .builder()
				  .config(conf)
				  .master("local[*]")
				  .appName("Java Spark ")
				  .getOrCreate();
		 
		List<NetworkSignal> result = new ArrayList<NetworkSignal>();

		try {
			
			Dataset<Row> df = spark.read().format("parquet")
					.load("C:/Vinay/data/*.parquet");
			
			df=	df.withColumn("time", df.col("data.time")).withColumn("networkType", df.col("data.networkType")).withColumn("rxSpeed", df.col("data.rxSpeed"))
				.withColumn("txSpeed", df.col("data.txSpeed")).withColumn("rxData", df.col("data.rxData"))
				.withColumn("txData", df.col("data.txData")).withColumn("latitude", df.col("data.latitude")).withColumn("longitude", df.col("data.longitude"))
				.drop("data");
				

/*
				
				Dataset<NetworkSignal>	df1 = df.select(df.col("time"),df.col("networkType"),df.col("rxSpeed"),df.col("txSpeed")
						,df.col("rxData"),df.col("txData"),df.col("latitude"),df.col("longitude"))
						
						.as(NetworkSignal.getHierarchicalElementEncoder());*/
				
				
			 List<Row> res = 	df.collectAsList();

			 
			  result = res.stream().map(row ->new NetworkSignal(row.getLong(0), row.getString(1), row.getDouble(2), row.getDouble(3),
					 row.getLong(4), row.getLong(5), row.getDouble(6), row.getDouble(7)) ).collect(Collectors.toList());
			 


				
				System.out.println("test "+result);
				
		
				
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	
	

	
	
	
}
