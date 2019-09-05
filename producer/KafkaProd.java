package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class KafkaProd {

	
	   private static final long INCOMING_DATA_INTERVAL = 10;

	  //  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProd.class);

	   // @Override
	    public void run() {
	     //   LOGGER.info("Initializing kafka producer...");

	        Properties properties = ConfigUtil.getConfig("producer");
	        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
	        properties.put(ProducerConfig.ACKS_CONFIG, "all");
	        properties.put(ProducerConfig.RETRIES_CONFIG, 0);

	        String topic = "livenetwork";
	      //  LOGGER.info("Start producing random network data to topic: " + topic);

	        Producer<String, String> producer = new KafkaProducer<>(properties);

	        Random random = new Random();

	       //Integer.MAX_VALUE
	        
	        final int deviceCount = 100;
	        List<String> deviceIds = IntStream.range(0, deviceCount)
	                                          .mapToObj(i-> UUID.randomUUID().toString())
	                                          .collect(Collectors.toList());
	       
	      
	        for (int i = 0; i < Integer.MAX_VALUE; i++) {
	          
	        	 NetworkData networkData = new NetworkData();
	        	  NetworkSignal networkSignal = new NetworkSignal();
	             networkData.setDeviceId(deviceIds.get(random.nextInt(deviceCount-1)));
	             networkData.setSignals(new ArrayList<>());
	             for (int j = 0; j < random.nextInt(4)+1; j++) {
	               
	                 networkSignal.setNetworkType(i % 2 == 0 ? "mobile" : "wifi");
	                 networkSignal.setRxData((long) random.nextInt(1000));
	                 networkSignal.setTxData((long) random.nextInt(1000));
	                 networkSignal.setRxSpeed((double) random.nextInt(100));
	                 networkSignal.setTxSpeed((double) random.nextInt(100));
	                 networkSignal.setTime(System.currentTimeMillis());
	                 networkSignal.setLatitude((Math.random() * 10) + 1);
	                 networkSignal.setLongitude((Math.random() * 10) + 1);
	                // networkData.getSignals().add(networkSignal);
	             }

	             String key = UUID.randomUUID().toString();
	             String value = networkSignal.toJson();
	            
	            

	       //     LOGGER.warn("Random data generated: {}: {}", key, value);

	            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
	            producer.send(record);

	            try {
	                if (INCOMING_DATA_INTERVAL > 0) {
	                    Thread.sleep(INCOMING_DATA_INTERVAL);
	                }
	            } catch (InterruptedException e) {
	                e.printStackTrace();
	            }
	        }

	        producer.close();
	    }
	    
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		 new KafkaProd().run();
	}

}
