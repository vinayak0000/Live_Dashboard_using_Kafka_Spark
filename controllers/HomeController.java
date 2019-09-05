package com.springKafka.liveDashboard.controllers;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.producer.NetworkSignal;
import com.springKafka.liveDashboard.services.SparkComponent;
import com.springKafka.liveDashboard.services.SparkService;

@RestController
public class HomeController {
	
	@Autowired
	private SparkService sparkService;
	
	@Autowired
	private SparkComponent sparkComponent;
	
	@GetMapping("/home")
	public String home(Model model) {
		return "home";
	}
	
	
	@RequestMapping(value = "/readFromFile", method = RequestMethod.GET)
	public List<NetworkSignal> readFromFile() {

	
		//Dataset<NetworkSignal> json = null;
		try {
			
		
				
			/*for(NetworkSignal n: json)
			{
				System.out.println(n.getLongitude());
			}*/

		} catch (Exception e) {
			e.printStackTrace();
		}
		return sparkService.readFromFile();
	}
	
	
	@RequestMapping(value = "/readKafkaStreams", method = RequestMethod.GET)
	public List<NetworkSignal> readKafkaStreams() {

		List<NetworkSignal> result= new ArrayList<NetworkSignal>();
	
		try {
			
		
			result =	sparkComponent.readKafkaStreams();

		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	
}
