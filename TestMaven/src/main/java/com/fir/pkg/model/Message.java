package com.fir.pkg.model;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

public class Message implements Serializable {

    @SerializedName(value="q")
    Integer quality;

    @SerializedName(value="v")
    Double value;

    @SerializedName(value="ts")
    String timestamp;

    public Message() {
    }

    public Message(Integer quality, Double value, String timestamp) {
        this.quality = quality;
        this.value = value;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    public Integer getQuality() {
        return quality;
    }

    public void setQuality(Integer quality) {
        this.quality = quality;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
}
