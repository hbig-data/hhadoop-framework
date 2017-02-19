package com.ryan.hadoop.proxy;

import org.mortbay.servlet.ProxyServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * @Author Rayn
 * @Vendor liuwei412552703@163.com
 * Created by Rayn on 2017/1/11 10:33.
 */
public class JettyAsyncServletProxy extends ProxyServlet {

    private static final Logger LOG = LoggerFactory.getLogger(JettyAsyncServletProxy.class);

    /**
     * Resolve requested URL to the Proxied URL
     *
     * @param scheme     The scheme of the received request.
     * @param serverName The server encoded in the received request(which
     *                   may be from an absolute URL in the request line).
     * @param serverPort The server port of the received request (which
     *                   may be from an absolute URL in the request line).
     * @param uri        The URI of the received request.
     * @return The URL to which the request should be proxied.
     * @throws MalformedURLException
     */
    @Override
    protected URL proxyHttpURL(String scheme, String serverName, int serverPort, String uri) throws MalformedURLException {

        super.proxyHttpURL(scheme, serverName, serverPort, uri);

        return new URL("http://www.baidu.com");
    }
}
