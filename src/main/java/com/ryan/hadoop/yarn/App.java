package com.ryan.hadoop.yarn;

import org.apache.hadoop.hdfs.web.resources.ExceptionHandler;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author Rayn
 * @email liuwei412552703@163.com
 * Created by Rayn on 2016/11/22 15:27.
 */
public class App {
    private static final Logger LOG = LoggerFactory.getLogger(App.class);

    private YarnClient yarnClient = null;
    private YarnClientApplication application = null;

    public App() {
        YarnConfiguration yarnConf = new YarnConfiguration();

        try {
            yarnClient = YarnClient.createYarnClient();
            yarnClient.init(yarnConf);
            yarnClient.start();

            LOG.info("启动 YarnClient 成功.");
        } catch (Exception e) {
            try {
                yarnClient.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            e.printStackTrace();
        }
    }

    /**
     * 运行APP
     */
    public void lanuchApp() {

        try {
            LOG.info("创建 Application .....");
            application = yarnClient.createApplication();
            GetNewApplicationResponse newApplicationResponse = application.getNewApplicationResponse();
            LOG.info("applicationId : {}", newApplicationResponse.getApplicationId());

        } catch (YarnException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 关闭Yarn
     */
    public void close() {

        if (null != yarnClient) {
            try {
                yarnClient.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }
    }






    public static void main(String[] args) {
        App app = new App();
        app.lanuchApp();


        app.close();
    }
}
