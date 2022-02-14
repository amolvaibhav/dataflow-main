package com.fir.pkg.model;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SampleClass implements Serializable {

    @SerializedName(value="fields")
    Map<String, List<MainSampleClass>> memberMap=new HashMap<>();

    public Map<String, List<MainSampleClass>> getMemberMap() {
        return memberMap;
    }

    public void setMemberMap(Map<String, List<MainSampleClass>> memberMap) {
        this.memberMap = memberMap;
    }

    public SampleClass() {
    }

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
