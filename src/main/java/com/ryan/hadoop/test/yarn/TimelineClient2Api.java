package com.ryan.hadoop.test.yarn;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timelineservice.TimelineEntity;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * @author Rayn on 2017/1/15.
 * @email liuwei412552703@163.com.
 */
public class TimelineClient2Api {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        // Create and start the Timeline client v.2
        TimelineClient client = TimelineClient.createTimelineClient();
        client.init(conf);
        client.start();

        try {
            TimelineEntity entity = new TimelineEntity();
            entity.setType("MY_APPLICATION");
            entity.setId("MyApp1");
            // Compose other entity info

            // Blocking write
            client.putEntities(entity);

            TimelineEntity myEntity2 = new TimelineEntity();
            // Compose other info

            // Non-blocking write
            client.putEntitiesAsync(myEntity2);

        } catch (IOException e) {
            // Handle the exception
        } catch (RuntimeException e) {
            // In Hadoop 2.6, if attempts submit information to the Timeline Server fail more than the retry limit,
            // a RuntimeException will be raised. This may change in future releases, being
            // replaced with a IOException that is (or wraps) that which triggered retry failures.
        } catch (YarnException e) {
            // Handle the exception
        } finally {
            // Stop the Timeline client
            client.stop();
        }
    }
}
