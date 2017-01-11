package com.ryan.hadoop.proxy;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.http.HttpRequestLog;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.RequestLog;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.SessionManager;
import org.mortbay.jetty.handler.ContextHandlerCollection;
import org.mortbay.jetty.handler.HandlerCollection;
import org.mortbay.jetty.handler.RequestLogHandler;
import org.mortbay.jetty.servlet.AbstractSessionManager;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.DefaultServlet;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.QueuedThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Author Rayn
 * @Vendor liuwei412552703@163.com
 * Created by Rayn on 2017/1/11 9:21.
 */
public class WebJettyProxyTestCase {
    private static final Logger LOG = LoggerFactory.getLogger(WebJettyProxyTestCase.class);


    private Server server = null;

    private WebAppContext ctx = null;


    /**
     * 初始化 Jetty Server
     */
    public void initServer() {
        server = new Server(8080);
        int maxThreads = 2;

        QueuedThreadPool threadPool = maxThreads == -1 ? new QueuedThreadPool() : new QueuedThreadPool(maxThreads);
        threadPool.setDaemon(true);
        server.setThreadPool(threadPool);

    }


    /**
     * 初始化Jetty WebAppContext
     */
    public void initWebAppContext() {
        this.ctx = new WebAppContext();

        /**
         * Session 管理
         */
        SessionManager sm = ctx.getSessionHandler().getSessionManager();
        if (sm instanceof AbstractSessionManager) {
            AbstractSessionManager asm = (AbstractSessionManager) sm;
            asm.setHttpOnly(true);
            asm.setSecureCookies(true);
        }

        ContextHandlerCollection contexts = new ContextHandlerCollection();
        RequestLog requestLog = HttpRequestLog.getRequestLog("proxy");

        if (requestLog != null) {
            RequestLogHandler requestLogHandler = new RequestLogHandler();
            requestLogHandler.setRequestLog(requestLog);
            HandlerCollection handlers = new HandlerCollection();
            handlers.setHandlers(new Handler[] {contexts, requestLogHandler});
            server.setHandler(handlers);
        } else {
            server.setHandler(contexts);
        }

        server.addHandler(ctx);
    }

    public void addStatic(){
        ContextHandlerCollection contexts = new ContextHandlerCollection();

        // set up the context for "/static/*"
        Context staticContext = new Context(contexts, "/static");
        staticContext.setResourceBase("/static");
        staticContext.addServlet(DefaultServlet.class, "/*");
        staticContext.setDisplayName("static");
    }

    /**
     *
     */
    public void run() {
        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void addServlet() {

        WebAppContext ctx = new WebAppContext();
        ctx.setDefaultsDescriptor(null);
        ctx.setContextPath("/");

        ServletHolder holder = new ServletHolder(new DefaultServlet());
        Map<String, String> params = ImmutableMap.<String, String>builder()
                .put("acceptRanges", "true")
                .put("dirAllowed", "false")
                .put("gzip", "true")
                .put("useFileMappedBuffer", "true")
                .build();
        holder.setInitParameters(params);
        ctx.setWelcomeFiles(new String[]{"index.html"});

        ctx.addServlet(holder, "/");

        ServletHolder jettyHolder = new ServletHolder(JettyAsyncServletProxy.class);
        ctx.addServlet(jettyHolder, "/proxy/*");

    }

    public static void main(String[] args) {

        WebJettyProxyTestCase webJettyProxyTestCase = new WebJettyProxyTestCase();
        webJettyProxyTestCase.initServer();

        webJettyProxyTestCase.initWebAppContext();


        webJettyProxyTestCase.run();

    }


}
