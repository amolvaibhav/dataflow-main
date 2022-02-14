import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fir.pkg.model.MainSampleClass;
import com.fir.pkg.model.Message;
import com.fir.pkg.model.SampleClass;
import com.fir.pkg.util.Utility;
import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.joda.time.DateTime;
import org.json.*;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MainApp {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        //projects/infoworks-326916/topics/real-time-data
        publishWithCustomAttributesExample("infoworks-326916","real-time-data");
    }

    public static void publishWithCustomAttributesExample(String projectId, String topicId)
            throws IOException, ExecutionException, InterruptedException {
        TopicName tName= TopicName.of(projectId, topicId);
        Publisher publisher=null;
        //List<String> listTags = List.of(new String[]{"ABC", "DEF", "GHI", "JKL", "MNO", "PQR", "STV", "XYZ"});
        String[] arrTags=new String[]{"ABC", "DEF", "GHI", "JKL", "MNO", "PQR", "STV", "XYZ"};
        List<String> listTags = Arrays.asList(arrTags);
        try{
            publisher = Publisher.newBuilder(tName).build();
            Type mapType=new TypeToken<HashMap<String, List<Message>>>(){}.getType();

            while (true) {
                HashMap<String, List<Message>> msgMap = new HashMap<>();
                Integer rand_idx=new Random().nextInt(listTags.size());
                String tag= listTags.get(rand_idx);

                List<Message> messageList=Utility.constructMessage();
                /*for (Map.Entry<String,List<Message>> entry: msgMap.entrySet()){
                    for (Message li:entry.getValue()){
                        System.out.println(entry.getKey()+" "+entry.getValue().toString());
                    }
                }*/
                String mainData="{\""+ tag + "\":" + messageList.toString() + "}";

                ByteString data = ByteString.copyFromUtf8(mainData);
                Integer idx=new Random().nextInt(2);
                if (idx%2==0){
                    PubsubMessage pubsubMessage=PubsubMessage.newBuilder()
                            .setData(data)
                            .putAllAttributes(ImmutableMap.of("key1","v1"))
                            .build();
                    //System.out.println("PubSub"+pubsubMessage);
                    ApiFuture<String> messageIdFuture=publisher.publish(pubsubMessage);
                    System.out.println("Published a message with custom attributes: " + messageIdFuture.get() + "Message "+ pubsubMessage);
                }else{
                    PubsubMessage pubsubMessage1=PubsubMessage.newBuilder()
                            .setData(data)
                            .putAllAttributes(ImmutableMap.of("key2","v2"))
                            .build();
                    //System.out.println("PubSub"+pubsubMessage1);
                    ApiFuture<String> messageIdFuture1=publisher.publish(pubsubMessage1);
                    System.out.println("Published a message with custom attributes: " + messageIdFuture1.get() + "Message "+ pubsubMessage1);
                }
                System.out.println(data);



            }
        }catch(Exception err){
            err.printStackTrace();
        }finally {
            if (publisher!=null){
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
    }
}
