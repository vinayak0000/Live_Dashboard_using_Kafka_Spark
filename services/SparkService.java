package com.springKafka.liveDashboard.services;

import java.util.List;

import com.kafka.producer.NetworkSignal;

public interface SparkService {

	public List<NetworkSignal> readFromFile();
}
