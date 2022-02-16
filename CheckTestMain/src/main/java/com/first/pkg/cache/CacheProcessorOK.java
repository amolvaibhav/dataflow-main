package com.first.pkg.cache;

import com.first.pkg.model.CacheModel;
import com.first.pkg.processor.MainProcessor;
import com.google.gson.Gson;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class CacheProcessorOK extends DoFn<Long, Map<String, String>> {

    private final static Logger log = Logger.getLogger(String.valueOf(CacheProcessorOK.class));
    String url;
    transient OkHttpClient client;

    public CacheProcessorOK(String url) {
        this.url = url;
        client = new OkHttpClient.Builder()
                .connectTimeout(300, TimeUnit.SECONDS)
                .readTimeout(300,TimeUnit.SECONDS)
                .build();
    }

    public void process(@Element Long element, OutputReceiver<Map<String, String>> o) throws IOException {

        Map<String, String> cacheMap = new HashMap<String, String>();

        try {
            Request request = new Request.Builder().url(this.url).build();

            Response response = client.newCall(request).execute();
            log.info("Response Code: " + response.code());

            String content = response.body().string();
            log.info("Data Received: " + content);

            if (response.code() == 200) {
                JSONArray responseArray = new JSONArray(content);
                /*for (int i = 0; i < responseArray.length(); i++) {
                    JSONObject singleObj = new JSONObject(responseArray.get(i));
                    cacheMap.put(singleObj.getString("tag"), singleObj.getString("data"));
                }*/
                for (int i=0;i<responseArray.length();i++){
                    CacheModel cacheModel = new Gson().fromJson(responseArray.get(i).toString(), CacheModel.class);
                    cacheMap.put(cacheModel.getTag(), cacheModel.getData());
                }
                o.output(cacheMap);
            } else {
                log.info("Cannot Connect with Service. Response Code: " + response.code());
                o.output(cacheMap);
            }
        }catch (Exception err){
            log.info("GENERAL EXCEPTION CAUGHT: "+ err.getMessage());
            err.printStackTrace();
        }
    }


}
