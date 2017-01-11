package com.ryan.hadoop.proxy;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.servlet.ProxyServlet;

/**
 * @Author Rayn
 * @Vendor liuwei412552703@163.com
 * Created by Rayn on 2017/1/11 11:19.
 */
public class JettyProxy {

    public static void main(String[] args) {
        Server server = new Server(8080);

        ServletHandler handler = new ServletHandler();
        server.setHandler(handler);

        ProxyServlet servlet = new JettyAsyncServletProxy();
        ServletHolder proxyServletHolder = new ServletHolder(servlet);
        proxyServletHolder.setInitParameter("maxThreads", "100");

        handler.addServletWithMapping(proxyServletHolder, "/*");


        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }
}
