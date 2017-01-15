package com.ryan.hadoop.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEntity;
import org.apache.hadoop.yarn.api.records.timeline.TimelineEvent;
import org.apache.hadoop.yarn.api.records.timeline.TimelinePutResponse;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * （1）持久化应用程序的具体信息
 * 收集并检索应用程序或者框架的具体信息。例如，hadoop MR框架里面的与分片线关系的信息，诸如map tasks、reduce tasks、counters等。应用程序开发者可以在App Master端或者应用程序需的containers中通过TimelineClient将这些信息发送给Timeline Server。
 * 这些信息都可以通过REST APIs在具体App或者执行框架的UI界面查询到。
 * （2）持久化已完成的应用程序的Generic information
 * 关于这一点，在Application history server中，显然只支持MR框架的job。Generic information包括了像*queue-name，*用户信息等用户程序级别的数据，还有设置在ApplicationSubmissionContext中的信息，用于运行应用程序的application-attempts
 * 列表，关于每个application-attempt的信息，container列表以及运行在每个 application-attempt *下的每个container的信息。
 *
 *
 *
 * (1)Timeline Domain
 Timeline Domain为Timeline Server提供了一个命令空间，使得用户可以搜集多个节点，将它们与其他用户和应用程序隔离开来。Timeline server security就定义在这一层。
 一个Domain首先是用于存储用户的信息、读写ACL信息、创建和修改时间戳。每个Domain以一个唯一的ID在整个YARN集群中标识。
 （2）Timeline Entity
 一个Timeline Entity（即Timeline实体）包含一个概念实体的元信息以及它的相关的events.一个实体可以是一个application，一个 application attempt，一个container卓尔其他任何的应用自定义的object。
 它还包含多个Primary filters用于作为timeline store中多个实体的索引。其他的数据可以以非索引的方式存储。每个实体都通过一个EntityId和EntityType唯一的确定。
 （3）Timeline Events
 Timeline Events用于描述一个与某个具体Application的timeline实体相关的event。用户也可以随意定义一个event方法，比如启动一个应用程序，获取分配的container、操作失败或者其他的与用户和集群操作相关的失败信息等等。



 * @author Rayn on 2017/1/15.
 * @email liuwei412552703@163.com.
 */
public class TimelineClientApi {

    public static void main(String[] args) {
        Configuration conf = new Configuration();

        // Create and start the Timeline client
        TimelineClient client = TimelineClient.createTimelineClient();
        client.init(conf);
        client.start();

        try {
            TimelineDomain myDomain = new TimelineDomain();
            myDomain.setId("MyDomain");
            // Compose other Domain info ....

            client.putDomain(myDomain);

            TimelineEntity myEntity = new TimelineEntity();
            myEntity.setDomainId(myDomain.getId());
            myEntity.setEntityType("APPLICATION");
            myEntity.setEntityId("MyApp1");
            // Compose other entity info

            TimelinePutResponse response = client.putEntities(myEntity);

            TimelineEvent event = new TimelineEvent();
            event.setEventType("APP_FINISHED");
            event.setTimestamp(System.currentTimeMillis());
            event.addEventInfo("Exit Status", "SUCCESS");
            // Compose other Event info ....

            myEntity.addEvent(event);


            TimelinePutResponse response1 = client.putEntities(myEntity);

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
