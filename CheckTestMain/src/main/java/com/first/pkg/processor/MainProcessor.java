package com.first.pkg.processor;

import com.first.pkg.model.EnrichedObject;
import com.first.pkg.model.Message;
import com.first.pkg.utility.Utility;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Logger;

public class MainProcessor {

    private final static Logger log = Logger.getLogger(String.valueOf(MainProcessor.class));

    public static class SegregateKVfromMap extends DoFn<Map<String,List<Message>>, KV<String,Message>>{

        @ProcessElement
        public void process(ProcessContext context){
            Map<String,List<Message>> inMap=context.element();
            for (Map.Entry<String,List<Message>> entry: inMap.entrySet()){
                for (Message m: entry.getValue()){
                    context.output(KV.of(entry.getKey(), m));
                }
            }
        }

    }
    public static class FinalVerifyObject implements Partition.PartitionFn<EnrichedObject> {

        @Override
        public @UnknownKeyFor @NonNull @Initialized int partitionFor(EnrichedObject elem, @UnknownKeyFor @NonNull @Initialized int numPartitions) {
            elem.setReason("");
            if ((Objects.isNull(elem.getTagID())) || (Objects.isNull(elem.getAssetID()))){
                elem.setReason("Not Found in cache");
            }
            return elem.getReason().equals("")?0:1;
        }
    }
    public static class LookupCache extends DoFn<Map<String, List<Message>>, List<EnrichedObject>>{

        //HashMap<String,String> cacheMap=new HashMap<>();
        PCollectionView<Map<String,String>> cacheMap;

        public LookupCache(PCollectionView<Map<String, String>> cacheMap) {
            this.cacheMap = cacheMap;
        }

        /*@Setup
                public void setup(){
                    this.cacheMap.put("ABC","{\"tid\":123,\"aid\":456,\"is_alarm\"=true}");
                    this.cacheMap.put("DEF","{\"tid\":1234,\"aid\":4567,\"is_alarm\"=true}");
                    this.cacheMap.put("JKL","{\"tid\":12345,\"aid\":45678,\"is_alarm\"=false}");
                    this.cacheMap.put("MNO","{\"tid\":123456,\"aid\":456789,\"is_alarm\"=true}");
                    this.cacheMap.put("PQR","{\"tid\":1234567,\"aid\":4567890,\"is_alarm\"=false}");
                }*/
        @ProcessElement
        public void process(ProcessContext context){
            try {
                List<EnrichedObject> finalList = new ArrayList<>();
                Map<String,String> cacheMap=context.sideInput(this.cacheMap);
                for (Map.Entry<String, List<Message>> entry : context.element().entrySet()) {
                    for (Message msg : entry.getValue()) {
                        String cacheResponse = cacheMap.get(entry.getKey());
                        log.info("Cached Response    "+cacheResponse);
                        if (Objects.isNull(cacheResponse)){
                            EnrichedObject emptyEnObj=new EnrichedObject(entry.getKey(), msg.getQuality(), msg.getValue(), msg.getTs());
                            finalList.add(emptyEnObj);
                        }else {
                            EnrichedObject enObj = new Gson().fromJson(cacheResponse, EnrichedObject.class);
                            log.info("Key Tag    " + entry.getKey());
                            enObj.setTagName(entry.getKey());
                            enObj.setValue(msg.getValue());
                            enObj.setQuality(msg.getQuality());
                            enObj.setTimestamp(msg.getTs());
                            finalList.add(enObj);
                        }
                    }
                }
                context.output(finalList);
            }catch (Exception e){
                log.warning("Exception      "+e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public static class ExtractMessage extends DoFn<String,Map<String, List<Message>>>{

        @ProcessElement
        public void process(ProcessContext context){
                Type myType = new TypeToken<Map<String, List<Message>>>() {
                }.getType();
                Map<String, List<Message>> msgMap = new Gson().fromJson(context.element(), myType);
                context.output(msgMap);
        }
    }
    public static class VerifyPayload extends DoFn<String,String>{

        @ProcessElement
        public void process(ProcessContext context){
            Type myType = new TypeToken<Map<String, List<Message>>>() {
            }.getType();
            try {
                Map<String, List<Message>> msgMap = new Gson().fromJson(context.element(), myType);
                for (Map.Entry<String,List<Message>> entry: msgMap.entrySet()){
                    log.info(entry.getKey()+"======"+entry.getValue().toString());
                }
                context.output(context.element());
            }catch (Exception err){
                log.warning(err.getMessage());
                err.printStackTrace();
                context.output(Utility.FAIL, context.element());
            }
        }
    }
}
