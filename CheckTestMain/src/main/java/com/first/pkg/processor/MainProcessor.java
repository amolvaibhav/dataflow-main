package com.first.pkg.processor;

import com.first.pkg.model.EnrichedObject;
import com.first.pkg.model.Message;
import com.first.pkg.utility.Utility;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Duration;

import java.awt.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.List;
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
            /*if (elem.getAssetID().equals(0) || elem.getTagID().equals(0)){
                elem.setReason("Not Found in Cache!!!");
            }*/
            return elem.getReason().equals("")?0:1;
        }
    }

    public static class LookupCache2 extends PTransform<PCollection<Map<String, List<Message>>>, PCollection<List<EnrichedObject>>> {

        PCollection<Map<String,String>> cacheMap;

        public LookupCache2(PCollection<Map<String, String>> cacheMap) {
            this.cacheMap =  cacheMap;
        }

        @Override
        public PCollection<List<EnrichedObject>> expand(PCollection<Map<String, List<Message>>> input) {

            PCollection<KV<String, String>> kvMessage=input.apply("Convert Map to KV", ParDo.of(new DoFn<Map<String, List<Message>>, KV<String, String>>() {

                @ProcessElement
                public void process(ProcessContext context){
                    for (Map.Entry<String,List<Message>> entry:context.element().entrySet()){
                        for (Message singleMessage : entry.getValue()){
                            context.output(KV.of(entry.getKey(), entry.getValue().toString()));
                        }
                    }
                }
            }));

            PCollection<KV<String, String>> kvCache=cacheMap.apply("Convert Cache Map to Cache KV",ParDo.of(new DoFn<Map<String, String>, KV<String, String>>() {

                @ProcessElement
                public void process(ProcessContext context){
                    for (Map.Entry<String,String> entry:context.element().entrySet()){
                        context.output(KV.of(entry.getKey(), entry.getValue()));
                    }
                }
            }));

            PCollection<KV<String, String>> windowedMessage = kvMessage.apply("Apply Window", Window.<KV<String, String>>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .triggering(
                            AfterWatermark.pastEndOfWindow()
                                    .withLateFirings(AfterProcessingTime
                                            .pastFirstElementInPane().plusDelayOf(Duration.standardHours(1)))).accumulatingFiredPanes()
                    .withAllowedLateness(Duration.standardDays(1)));

            PCollection<KV<String, String>> windowedCache = kvCache.apply("Apply Window", Window.<KV<String, String>>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .triggering(
                            AfterWatermark.pastEndOfWindow()
                                    .withLateFirings(AfterProcessingTime
                                            .pastFirstElementInPane().plusDelayOf(Duration.standardHours(1)))).accumulatingFiredPanes()
                    .withAllowedLateness(Duration.standardDays(1)));

            PCollection<KV<String, CoGbkResult>> gbkResultSet = KeyedPCollectionTuple.of(Utility.MESSAGE, windowedMessage).and(Utility.CACHE, windowedCache)
                    .apply(CoGroupByKey.create());

            PCollection<List<EnrichedObject>> finalPcollection= gbkResultSet.apply("Process Groups",ParDo.of(new DoFn<KV<String, CoGbkResult>, List<EnrichedObject>>() {

                @ProcessElement
                public void process(ProcessContext context){
                    KV<String, CoGbkResult> element=context.element();
                    String tag=element.getKey();
                    String cacheValue=element.getValue().getOnly(Utility.CACHE);
                    String messageValue=element.getValue().getOnly(Utility.MESSAGE);
                    List<EnrichedObject> finalList=new ArrayList<>();
                    for (String str: element.getValue().getAll(Utility.MESSAGE)){
                        EnrichedObject enrichedObject=new Gson().fromJson(cacheValue,EnrichedObject.class);
                        Type myType=new TypeToken<Map<String,List<Message>>>(){}.getType();
                        Map<String,List<Message>> o = new Gson().fromJson(str, myType);
                        for (Map.Entry<String,List<Message>> entry:o.entrySet()){
                            for (Message singleMessage:entry.getValue()){
                                enrichedObject.setTagName(entry.getKey());
                                enrichedObject.setQuality(singleMessage.getQuality());
                                enrichedObject.setValue(singleMessage.getValue());
                                enrichedObject.setTimestamp(singleMessage.getTs());
                                finalList.add(enrichedObject);
                            }
                        }
                    }

                }
            }));

            return finalPcollection;
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
