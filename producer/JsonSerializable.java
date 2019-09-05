package com.kafka.producer;

import java.io.Serializable;

import com.google.gson.Gson;

public abstract class JsonSerializable implements Serializable {

    private static final Gson GSON = new Gson();
    
    public String toJson() {
        return GSON.toJson(this);
    }

}