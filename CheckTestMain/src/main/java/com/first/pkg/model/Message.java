package com.first.pkg.model;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

public class Message implements Serializable {

    @SerializedName(value="ts")
    public String ts;

    @SerializedName(value="q")
    public Integer quality;

    @SerializedName(value="v")
    public Double value;

    public Message() {
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    public Message(String ts, Integer quality, Double value) {
        this.ts = ts;
        this.quality = quality;
        this.value = value;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
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
}
