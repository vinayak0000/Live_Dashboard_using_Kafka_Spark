package com.springKafka.liveDashboard.services;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import com.kafka.producer.NetworkSignal;

@Component
public class SparkComponent implements CommandLineRunner {

	
	SparkSession 	 spark = SparkSession	
			  .builder()
				
			  .master("local[*]")
			  .appName("Java Spark ")
			  .getOrCreate();
	
	Dataset<Row>  df ;
	
	@Override
	public void run(String... args) throws Exception {
		// TODO Auto-generated method stub
	
		StructField[] structFields = new StructField[]{
		           new StructField("time", DataTypes.LongType, true, Metadata.empty()),
		           new StructField("networkType", DataTypes.StringType, true, Metadata.empty()),
		           new StructField("rxSpeed", DataTypes.DoubleType, true, Metadata.empty()),
		           new StructField("txSpeed", DataTypes.DoubleType, true, Metadata.empty()),
		           new StructField("rxData", DataTypes.LongType, true, Metadata.empty()),
		           new StructField("txData", DataTypes.LongType, true, Metadata.empty()),
		           new StructField("latitude", DataTypes.DoubleType, true, Metadata.empty()),
		           new StructField("longitude", DataTypes.DoubleType, true, Metadata.empty()),
		   };

		   StructType structType = new StructType(structFields);
		   
		df = spark
			    .read()
			    .format("kafka")
			    .option("kafka.bootstrap.servers", "localhost:9092")
			    .option("subscribe", "livenetwork")
			    .load()
			        .selectExpr( "CAST(value AS STRING)")
			        .select(functions.from_json(functions.col("value"),structType).as("data"));
			       
		
	/*	 
		df.writeStream()
	      .outputMode("append")
	      .format("memory")
	      .queryName("network")
	      .start();
	*/
		df.createOrReplaceTempView("network");
		
		//spark.table("network").show();
	}

	
	public List<NetworkSignal> readKafkaStreams() {
		
		/*		//	Queries with streaming sources must be executed with writeStream.start()
		Dataset<Row> df  = spark.sql("SELECT * FROM network");
		
		df.show();*/
		List<NetworkSignal> result = new ArrayList<NetworkSignal>();
		//System.out.println("testing");
		
		 df  = spark.sql("SELECT * FROM network");
	//	df= spark.table("network");
		
		
	 df=	df.withColumn("time", df.col("data.time")).withColumn("networkType", df.col("data.networkType")).withColumn("rxSpeed", df.col("data.rxSpeed"))
				.withColumn("txSpeed", df.col("data.txSpeed")).withColumn("rxData", df.col("data.rxData"))
				.withColumn("txData", df.col("data.txData")).withColumn("latitude", df.col("data.latitude")).withColumn("longitude", df.col("data.longitude"))
				.drop(df.col("data"));
 
	 

	 List<Row> res = 	df.collectAsList();
	 

	// df.show();

		   result = res.stream().map(row ->new NetworkSignal( row.getLong(0), row.getString(1), row.getDouble(2), row.getDouble(3),
				   row.getLong(4), row.getLong(5), row.getDouble(6), row.getDouble(7)) ).collect(Collectors.toList()
						   
						   );
				   
					  
	//   System.out.println(result);
	   
		   return result;
		
	}
	


}


/*
StructField[] structFields = new StructField[]{
           new StructField("time", DataTypes.LongType, true, Metadata.empty()),
           new StructField("networkType", DataTypes.StringType, true, Metadata.empty()),
           new StructField("rxSpeed", DataTypes.DoubleType, true, Metadata.empty()),
           new StructField("txSpeed", DataTypes.DoubleType, true, Metadata.empty()),
           new StructField("rxData", DataTypes.LongType, true, Metadata.empty()),
           new StructField("txData", DataTypes.LongType, true, Metadata.empty()),
           new StructField("latitude", DataTypes.DoubleType, true, Metadata.empty()),
           new StructField("longitude", DataTypes.DoubleType, true, Metadata.empty()),
   };

   StructType structType = new StructType(structFields);
   

Dataset<Row> rfDataset = spark.createDataFrame(res, structType);
rfDataset=	rfDataset.withColumn("time", df.col("value.time")).withColumn("networkType", df.col("value.networkType")).withColumn("rxSpeed", df.col("value.rxSpeed"))
			.withColumn("txSpeed", df.col("value.txSpeed")).withColumn("rxData", df.col("value.rxData"))
			.withColumn("txData", df.col("value.txData")).withColumn("latitude", df.col("value.latitude")).withColumn("longitude", df.col("value.longitude"))
			;
res = rfDataset.collectAsList();*/
