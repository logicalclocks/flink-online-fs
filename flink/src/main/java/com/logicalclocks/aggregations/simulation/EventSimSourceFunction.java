package com.logicalclocks.aggregations.simulation;

import com.logicalclocks.aggregations.avroSchemas.StoreEvent;
import com.logicalclocks.aggregations.avroSchemas.contexts;
import com.logicalclocks.aggregations.avroSchemas.definitions;
import com.logicalclocks.aggregations.avroSchemas.sessionContext;
import com.logicalclocks.aggregations.avroSchemas.userContext;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class EventSimSourceFunction implements SourceFunction<Tuple2<String, StoreEvent>> {

  private final Random random = new Random();
  private final Random randType = new Random();
  private final Random randGender = new Random();
  private final Random randEvent = new Random();
  private final Random randTouchpoint = new Random();

  private long eventTime;
  private String customerId;
  private StoreEvent event;

  public static final int SLEEP_MILLIS_PER_EVENT = 5;
  private static final int BATCH_SIZE = 120;
  private static final  List<String> eventTypes = Arrays.asList("PAGE LOADED", "ADD_TO_BAG");
  private static final  List<String> genders = Arrays.asList("Male", "Female");
  private static final  List<String> touchPoints = Arrays.asList("Male", "Female");
  private volatile boolean running = true;

  @Override
  public void run(SourceContext<Tuple2<String, StoreEvent>> sourceContext) throws Exception {
    long id = 0;
    long maxStartTime = 0;

    while (running) {
      // generate a batch of events
      List<StoreEvent> events = new ArrayList<StoreEvent>(BATCH_SIZE);
      for (int i = 1; i <= BATCH_SIZE; i++) {
        eventTime =  Instant.now().toEpochMilli();
        customerId = hashIdGenerator("customer_id");
        event = stringEventGenerator(customerId, genderGenerator(),
            eventIdGenerator("event_id"),
            eventTypeGenerator(),
            touchPointGenerator(),
            timestampGenerator(eventTime));
        events.add(event);
        maxStartTime = Math.max(maxStartTime, eventTime);
      }

      events
          .iterator()
          .forEachRemaining(r -> sourceContext.collectWithTimestamp( new Tuple2<>(customerId, event), eventTime));

      // produce a Watermark
      sourceContext.emitWatermark(new Watermark(maxStartTime));

      // prepare for the next batch
      id += BATCH_SIZE;

      // don't go too fast
      Thread.sleep(SLEEP_MILLIS_PER_EVENT); //BATCH_SIZE * SLEEP_MILLIS_PER_EVENT

    }
  }

  @Override
  public void cancel() {
  }

  private String hashIdGenerator(String type) {
    int min = 1;
    int max = 100000;
    int ranfromNumber = random.nextInt(max - min) + min;
    return DigestUtils.sha256Hex(type + ranfromNumber);
  }

  private String eventIdGenerator(String type) {
    int min = 1;
    int max = 10000000;
    int ranfromNumber = randEvent.nextInt(max - min) + min;
    return DigestUtils.sha256Hex(type + ranfromNumber);
  }

  private String timestampGenerator(long timestamp){
    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
    return dateFormat.format(timestamp);
  }

  private String eventTypeGenerator() {
    int choice = randType.nextInt(eventTypes.size());
    return eventTypes.get(choice);
  }

  private String genderGenerator() {
    int choice = randGender.nextInt(genders.size());
    return genders.get(choice);
  }

  private String touchPointGenerator() {
    int choice = randTouchpoint.nextInt(touchPoints.size());
    return touchPoints.get(choice);
  }

  private StoreEvent stringEventGenerator(String customerId, String gender, String eventId, String eventType,
                                      String touchPoint, String receivedTs) {

    /*
    String template = "{\"event_type\": \""+ eventType + "\", \"event_id\": \""+ eventId +"\", " +
        "\"definitions\": {\"contexts\": {\"userContext\": {\"customer_id\": \""+ customerId +"\", " +
        "\"customer_gender\": \"" + gender + "\"}, " +
        "\"sessionContext\": {\"touchpoint\": \""+ touchPoint +"\"}}}, \"received_ts\": \"" +receivedTs+ "\"}";
     */
    StoreEvent storeEvent = new StoreEvent();

    storeEvent.setEventType(eventType);
    storeEvent.setEventId(eventId);
    storeEvent.setReceivedTs(receivedTs);

    definitions eventDefinitions = new definitions();
    contexts eventContext = new contexts();

    // user context
    userContext eventUserContext = new userContext();
    eventUserContext.setCustomerId(customerId);
    eventUserContext.setCustomerGender(gender);
    eventContext.setUserContext(eventUserContext);

    // session context
    sessionContext eventSessionContext = new sessionContext();
    eventSessionContext.setTouchpoint(touchPoint);
    eventContext.setSessionContext(eventSessionContext);

    // set event contexts
    eventDefinitions.setContexts(eventContext);

    // set definitions
    storeEvent.setEventDefinitions(eventDefinitions);

    return storeEvent;
  }
}
