package com.kafka.producer;

import java.io.Serializable;

import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;

public class NetworkSignal extends JsonSerializable implements Serializable {

    private static final long serialVersionUID = -4820697677113123242L;
    
    private static final Encoder<NetworkSignal> NETWORK_ELEMENT_ENCODER =
    	      Encoders.bean(NetworkSignal.class);
    

    private Long time;
    private String networkType;
    private Double rxSpeed;
    private Double txSpeed;
    private Long rxData;
    private Long txData;
    private Double latitude;
    private Double longitude;
    
    public NetworkSignal() {
    }
    
    public NetworkSignal(Long time, String networkType, Double rxSpeed, Double txSpeed, Long rxData, Long txData,
			Double latitude, Double longitude) {
		
		this.time = time;
		this.networkType = networkType;
		this.rxSpeed = rxSpeed;
		this.txSpeed = txSpeed;
		this.rxData = rxData;
		this.txData = txData;
		this.latitude = latitude;
		this.longitude = longitude;
	}

	public Long getTime() {
        return time;
    }

    public NetworkSignal setTime(Long time) {
        this.time = time;
        return this;
    }

    public String getNetworkType() {
        return networkType;
    }

    public NetworkSignal setNetworkType(String networkType) {
        this.networkType = networkType;
        return this;
    }

    public Double getRxSpeed() {
        return rxSpeed;
    }

    public NetworkSignal setRxSpeed(Double rxSpeed) {
        this.rxSpeed = rxSpeed;
        return this;
    }

    public Double getTxSpeed() {
        return txSpeed;
    }

    public NetworkSignal setTxSpeed(Double txSpeed) {
        this.txSpeed = txSpeed;
        return this;
    }

    public Long getRxData() {
        return rxData;
    }

    public NetworkSignal setRxData(Long rxData) {
        this.rxData = rxData;
        return this;
    }

    public Long getTxData() {
        return txData;
    }

    public NetworkSignal setTxData(Long txData) {
        this.txData = txData;
        return this;
    }

    public Double getLatitude() {
        return latitude;
    }

    public NetworkSignal setLatitude(Double latitude) {
        this.latitude = latitude;
        return this;
    }

    public Double getLongitude() {
        return longitude;
    }

    public NetworkSignal setLongitude(Double longitude) {
        this.longitude = longitude;
        return this;
    }
    
    public static Encoder<NetworkSignal> getHierarchicalElementEncoder() {
        return NETWORK_ELEMENT_ENCODER;
      }

	@Override
	public String toString() {
		return "NetworkSignal [time=" + time + ", networkType=" + networkType + ", rxSpeed=" + rxSpeed + ", txSpeed="
				+ txSpeed + ", rxData=" + rxData + ", txData=" + txData + ", latitude=" + latitude + ", longitude="
				+ longitude + "]";
	}
    
    

}