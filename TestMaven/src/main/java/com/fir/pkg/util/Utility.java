package com.fir.pkg.util;

import com.fir.pkg.model.Message;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

public class Utility {

    public static List<Message> constructMessage(){

        List<Message> listofMessage=new ArrayList<>();
        Integer i=new Random().nextInt(51);
        if ((i>40) && (i<51)){
            String currTimestamp= DateTime.now().toString();
            currTimestamp=currTimestamp.replace("+05:30","Z");
            Message newMsg=new Message(new Random().nextInt(201),new Random().nextDouble(), currTimestamp);
            Message newMsg2=new Message(new Random().nextInt(201),new Random().nextDouble(), currTimestamp);
            listofMessage.add(newMsg);listofMessage.add(newMsg2);
        }else {
            String currTimestamp = DateTime.now().toString();
            currTimestamp = currTimestamp.replace("+05:30", "Z");
            Message newMsg = new Message(new Random().nextInt(201), new Random().nextDouble(), currTimestamp);
            listofMessage.add(newMsg);
        }
        return listofMessage;
    }

    public static Map<String, Object> toMap(JSONObject jsonobj)  throws JSONException {
        Map<String, Object> map = new HashMap<String, Object>();
        Iterator<String> keys = jsonobj.keys();
        while(keys.hasNext()) {
            String key = keys.next();
            Object value = jsonobj.get(key);
            if (value instanceof JSONArray) {
                value = toList((JSONArray) value);
            } else if (value instanceof JSONObject) {
                value = toMap((JSONObject) value);
            }
            map.put(key, value);
        }   return map;
    }

    public static List<Object> toList(JSONArray array) throws JSONException {
        List<Object> list = new ArrayList<Object>();
        for(int i = 0; i < array.length(); i++) {
            Object value = array.get(i);
            if (value instanceof JSONArray) {
                value = toList((JSONArray) value);
            }
            else if (value instanceof JSONObject) {
                value = toMap((JSONObject) value);
            }
            list.add(value);
        }   return list;
    }
}
