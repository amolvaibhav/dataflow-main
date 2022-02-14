package com.first.pkg.processor;

import com.first.pkg.model.EnrichedObject;
import com.first.pkg.model.Message;
import com.first.pkg.utility.Utility;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.lang.reflect.Type;
import java.util.*;
import java.util.logging.Logger;

public class MainProcessor {

    private final static Logger log = Logger.getLogger(String.valueOf(MainProcessor.class));

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

        HashMap<String,String> cacheMap=new HashMap<>();

        @Setup
        public void setup(){
            this.cacheMap.put("ABC","{\"tid\":123,\"aid\":456,\"is_alarm\":true}");
            this.cacheMap.put("DEF","{\"tid\":1234,\"aid\":4567,\"is_alarm\":true}");
            this.cacheMap.put("JKL","{\"tid\":12345,\"aid\":45678,\"is_alarm\":false}");
            this.cacheMap.put("MNO","{\"tid\":123456,\"aid\":456789,\"is_alarm\":true}");
            this.cacheMap.put("PQR","{\"tid\":1234567,\"aid\":4567890,\"is_alarm\":false}");
        }
        @ProcessElement
        public void process(ProcessContext context){
            try {
                List<EnrichedObject> finalList = new ArrayList<>();
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
            String input= context.element();
            Type myType = new TypeToken<Map<String, List<Message>>>() {
            }.getType();
            try {
                Map<String, List<Message>> msgMap = new Gson().fromJson(context.element(), myType);
                for (Map.Entry<String,List<Message>> entry: msgMap.entrySet()){
                    log.info(entry.getKey()+"======"+entry.getValue().toString());
                }
                context.output(input);
            }catch (Exception err){
                log.warning(err.getMessage());
                err.printStackTrace();
                context.output(Utility.FAIL, input);
            }
        }
    }


    static class ExtractEventDataFn extends DoFn<TableRow, KV<String,TableRow>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row1 = new TableRow();
            TableRow row = c.element();
            row1.set("countryCode",row.get("countryCode"));
            row1.set("sqlDate",row.get("sqlDate"));
            row1.set("actor1Name",row.get("actor1Name"));
            row1.set("sourceUrl",row.get("sourceUrl"));
            String eventInfo = "Date: " + row1.get("sqlDate") + ", Actor1: " + row1.get("actor1Name") + ", url: " + row1.get("sourceUrl");
            c.output(KV.of((String)row1.get("countryCode"),row1));
        }
    }
    static class ExtractCountryInfoFn extends DoFn<TableRow, KV<String,TableRow>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow row1 = new TableRow();
            TableRow row = c.element();
            row1.set("countryCode",row.get("countryCode"));
            row1.set("countryName",row.get("countryName"));
            c.output(KV.of((String)row1.get("countryCode"),row1));
        }
    }
    public static PCollection<TableRow> joinEvents(
            PCollection<TableRow> eventsTable, PCollection<TableRow> countryCodes) throws Exception {

        final TupleTag<TableRow> eventInfoTag = new TupleTag<>();
        final TupleTag<TableRow> countryInfoTag = new TupleTag<>();

        // transform both input collections to tuple collections, where the keys are country
        // codes in both cases.
        PCollection<KV<String,TableRow>> eventInfo =
                eventsTable.apply(ParDo.of(new ExtractEventDataFn()));
        PCollection<KV<String,TableRow>> countryInfo =
                countryCodes.apply(ParDo.of(new ExtractCountryInfoFn()));

        // country code 'key' -> CGBKR (<event info>, <country name>)
        PCollection<KV<String, CoGbkResult>> kvpCollection =
                KeyedPCollectionTuple.of(eventInfoTag, eventInfo)
                        .and(countryInfoTag, countryInfo)
                        .apply(CoGroupByKey.create());

        // Process the CoGbkResult elements generated by the CoGroupByKey transform.
        // country code 'key' -> string of <event info>, <country name>
        PCollection<TableRow> finalResultCollection =
                kvpCollection.apply(
                        "Process",
                        ParDo.of(
                                new DoFn<KV<String, CoGbkResult>,TableRow>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {

                                        KV<String, CoGbkResult> e = c.element();
                                        String countryCode = e.getKey();
                                        TableRow countryName ;
                                        countryName = e.getValue().getOnly(countryInfoTag);


                                        // new1.set("HumanName", countryName.get("HumanName"));
                                        for (TableRow eventInfo : c.element().getValue().getAll(eventInfoTag)) {
                                            TableRow new1=new TableRow();
                                            new1.set("countryCode",countryName.get("countryCode"));
                                            new1.set("countryName", countryName.get("countryName"));
                                            new1.set("sqlDate", eventInfo.get("sqlDate"));
                                            new1.set("actor1Name", eventInfo.get("actor1Name"));
                                            new1.set("sourceUrl", eventInfo.get("sourceUrl"));
                                            System.out.println(new1.get("countryCode"));
                                            // Generate a string that combines information from both collection values
                                            c.output(new1);
                                        }
                                    }
                                }));

        return finalResultCollection;
    }

}
