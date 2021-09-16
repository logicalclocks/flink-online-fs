package com.logicalclocks.aggregations;

import org.apache.commons.codec.digest.DigestUtils;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Bluh {
  private final Random random = new Random();
  private final Random randType = new Random();
  private final Random randGender = new Random();
  private final Random randEvent = new Random();
  private final Random randTouchpoint = new Random();

  private long eventTime;
  private String customerId;
  private String eventString;

  public static final int SLEEP_MILLIS_PER_EVENT = 5;
  private static final int BATCH_SIZE = 120;
  private static final List<String> eventTypes = Arrays.asList("PAGE LOADED", "ADD_TO_BAG");
  private static final  List<String> genders = Arrays.asList("Male", "Female");
  private static final  List<String> touchPoints = Arrays.asList("Male", "Female");
  private volatile boolean running = true;

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

  private String stringEventGenerator(String customerId, String gender, String eventId, String eventType,
                                      String touchPoint, String receivedTs) {
    String template = "{\"event_type\": \""+ eventType + "\", \"event_id\": \""+ eventId +"\", " +
        "\"definitions\": {\"contexts\": {\"userContext\": {\"customer_id\": \""+ customerId +"\", " +
        "\"customer_gender\": \"" + gender + "\"}, " +
        "\"sessionContext\": {\"touchpoint\": \""+ touchPoint +"\"}}}, \"received_ts\": \"" +receivedTs+ "\"}";
    return template;
  }

  public void run() {
    eventTime =  Instant.now().toEpochMilli();
    customerId = hashIdGenerator("customer_id");
    eventString = stringEventGenerator(customerId, genderGenerator(),
        eventIdGenerator("event_id"),
        eventTypeGenerator(),
        touchPointGenerator(),
        timestampGenerator(eventTime));

    System.out.println(eventString);
  }

  public static void main(String[] args) {
    Bluh bluh = new Bluh();
    bluh.run();
  }
}
