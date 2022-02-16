package com.first.pkg.model;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.apache.kafka.common.protocol.types.Field;

import java.io.Serializable;

public class EnrichedObject implements Serializable {

    public String tagName;

    @SerializedName(value = "q")
    public Integer quality;

    @SerializedName(value = "v")
    public Double value;
    @SerializedName(value = "ts")
    public String timestamp;

    public String reason;

    @SerializedName(value="tid")
    public Integer tagID;

    @SerializedName(value="aid")
    public Integer assetID;

    @SerializedName(value = "ia_alarm")
    public boolean isAlarm;

    public EnrichedObject() {
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }

    public EnrichedObject(String tagName, Integer quality, Double value, String timestamp) {
        this.tagName = tagName;
        this.quality = quality;
        this.value = value;
        this.timestamp = timestamp;
    }

    public EnrichedObject(String tagName, Integer quality, Double value, String timestamp, Integer tagID, Integer assetID, boolean isAlarm, String reason) {
        this.tagName = tagName;
        this.quality = quality;
        this.value = value;
        this.timestamp = timestamp;
        this.tagID = tagID;
        this.assetID = assetID;
        this.isAlarm = isAlarm;
        this.reason=reason;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getTagName() {
        return tagName;
    }

    public void setTagName(String tagName) {
        this.tagName = tagName;
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

    public Integer getTagID() {
        return tagID;
    }

    public void setTagID(Integer tagID) {
        this.tagID = tagID;
    }

    public Integer getAssetID() {
        return assetID;
    }

    public void setAssetID(Integer assetID) {
        this.assetID = assetID;
    }

    public boolean isAlarm() {
        return isAlarm;
    }

    public void setAlarm(boolean alarm) {
        isAlarm = alarm;
    }
}
