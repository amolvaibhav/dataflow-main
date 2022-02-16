package com.first.pkg.cache;

import com.first.pkg.model.CacheModel;
import com.first.pkg.options.MyOptions;
import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;
//import com.google.cloud.teleport.templates.common.JavascriptTextTransformer.FailsafeJavascriptUdf;

public class CacheProcessor extends DoFn<Long, Map<String, String>> {

    private final static Logger log = Logger.getLogger(String.valueOf(CacheProcessorOK.class));
    String url;

    public CacheProcessor(String url) {
        this.url = url;
    }

    @ProcessElement
    public void process(@Element Long element, OutputReceiver<Map<String, String>> o) throws IOException {

        try {
            HashMap<String,String> cacheMap=new HashMap<>();
            URL url = new URL(this.url);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("User-Agent", "Mozilla/5.0");
            conn.setConnectTimeout(300 * 1000);
            conn.setReadTimeout(300 * 1000);

            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader buffReader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                String inputLine;
                StringBuffer strBuff = new StringBuffer();

                while ((inputLine = buffReader.readLine()) != null) {
                    strBuff.append(inputLine);
                }
                buffReader.close();
                String fullResponse = strBuff.toString();

                log.info("Response: "+fullResponse);
                /*JSONArray responseArray = new JSONArray(fullResponse);
                for (int i = 0; i < responseArray.length(); i++) {
                    JSONObject singleObj = new JSONObject(responseArray.get(i));
                    cacheMap.put(singleObj.get("tag")+"", singleObj.get("data")+"");
                }*/

                JSONArray responseArray=new JSONArray(fullResponse);
                for (int i=0;i<responseArray.length();i++){
                    CacheModel cacheModel = new Gson().fromJson(responseArray.get(i).toString(), CacheModel.class);
                    cacheMap.put(cacheModel.getTag(), cacheModel.getData());
                }
            }
            o.output(cacheMap);
        }catch (Exception err){
            log.info("GENERAL EXCEPTION CAUGHT!!! "+err.getMessage());
            err.printStackTrace();
        }
    }
}
