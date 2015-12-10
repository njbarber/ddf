/**
 * Copyright (c) Codice Foundation
 *
 * This is free software: you can redistribute it and/or modify it under the terms of the GNU Lesser
 * General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details. A copy of the GNU Lesser General Public License
 * is distributed along with this program and can be found at
 * <http://www.gnu.org/licenses/lgpl.html>.
 *
 **/
package org.codice.ddf.sdk.cometd;

import java.net.MalformedURLException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.bayeux.client.ClientSessionChannel.MessageListener;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to establish a session with the DDF cometd api. It provides the capability to monitor
 * notifications, activities as well as the ability to issue queries and download products.
 */
public class AsyncClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncClient.class);

    private static final String ACTIVITIES_CHANNEL = "/ddf/activities";
    private static final String ALL_ACTIVITIES = ACTIVITIES_CHANNEL + "/**";
    private static final String NOTIFICATIONS_CHANNEL = "/ddf/notifications";
    private static final String ALL_NOTIFICATIONS = NOTIFICATIONS_CHANNEL + "/**";
    private static final String DOWNLOADS_CHANNEL = NOTIFICATIONS_CHANNEL + "/downloads";
    private static final String QUERY_SERVICE = "/service/query";
    private static final String COMETD_CONTEXT = "/cometd";
    private static final String DOWNLOAD_CONTEXT = "/services/catalog/sources/";
    private static final String DOWNLOAD_TRANSFORM = "?transform=resource";

    private final String url;
    private final BayeuxClient client;
    private final Map<String, Object> emptyMessage = new HashMap<>();

    private String asyncClientId;
    private Map<String, Object> queryResponse;
    private Map<String, Object> activities;
    private Map<String, Object> notifications;

    /**
     * Creates a ddf async client with default channel subscriptions
     * @param url - target ddf url
     * @param disableValidation - disable ssl hostname validation
     * @throws Exception
     */
    public AsyncClient(String url, Boolean disableValidation) throws Exception {

        this.url = url;

        HttpClient httpClient = new HttpClient();

        httpClient.start();

        Map<String, Object> options = new HashMap<>();
        ClientTransport transport = new LongPollingTransport(options, httpClient);

        // DisableValidation
        if (disableValidation) {
            doTrustAllCertificates();
        }

        this.client = new BayeuxClient(url + COMETD_CONTEXT, transport);

        client.handshake(new MessageListener() {
            public void onMessage(ClientSessionChannel channel, Message message) {
                if (message.isSuccessful()) {
                    asyncClientId = channel.getSession().getId();

                    client.getChannel(ALL_ACTIVITIES).addListener((MessageListener)
                            (activitiesChannel, activitiesMessages) ->
                                    activities = activitiesMessages.getDataAsMap());

                    client.getChannel(ALL_NOTIFICATIONS).addListener((MessageListener)
                            (notificationsChannel, notificationsMessages) ->
                                    notifications = notificationsMessages.getDataAsMap());
                }
            }
        });
        boolean handshaken = client.waitFor(1000, BayeuxClient.State.CONNECTED);

        checkAllActivities();
        checkAllNotifications();
    }

    /**
     * Get all notification messages
     * @return - notifications map
     */
    public Map<String, Object> getNotifications() {
        return notifications;
    }

    /**
     * Get all activities messages
     * @return - activities map
     */
    public Map<String, Object> getActivities() {
        return activities;
    }

    /**
     * Get all query response messages
     * @return - query response map
     */
    public Map<String, Object> getQueryResponse() {
        return queryResponse;
    }

    /**
     * Retrieves all persisted notification.
     * The notifications channel returns any persisted notifications when it receives an empty message on the channel
     */
    public void checkAllNotifications() {

        client.getChannel(NOTIFICATIONS_CHANNEL).publish(emptyMessage);
    }

    /**
     * Retrieves download notifications
     */
    public void checkDownloadNotifications() {

        client.getChannel(DOWNLOADS_CHANNEL).publish(emptyMessage);
    }

    /**
     * Retrieves all persisted activities.
     * The activities channel returns any persisted activities when it receives an empty message on the channel
     */
    public void checkAllActivities() {

        client.getChannel(ACTIVITIES_CHANNEL).publish(emptyMessage);
    }

    /**
     * Execute a query against the ddf via the cometd endpoint
     * @param keyword - Simple keyword for query
     * @return - Query response map
     * @throws InterruptedException
     */
    public void query(String keyword) throws InterruptedException {
        String id = UUID.randomUUID().toString();
        String responseChannel = "/" + id;
        Map<String, Object> request = new HashMap<>();
        request.put("id", id);
        request.put("cql", "anyText ILIKE  '" + keyword + "'");

        client.getChannel(responseChannel).addListener((MessageListener)
                (channel, message) -> queryResponse = message.getDataAsMap());

        client.getChannel(QUERY_SERVICE).publish(request);
    }

    /**
     * Initiate download of a product.
     * @param catalogId - Catalog ID of the product to download.
     * @param sourceName - Name of the source to download the product from.
     * @throws MalformedURLException
     */
    public void downloadById(String catalogId, String sourceName) throws MalformedURLException {

        String downloadUrl = url + DOWNLOAD_CONTEXT + sourceName + "/" + catalogId +
                DOWNLOAD_TRANSFORM + "&session=" + asyncClientId;
        Thread downloadManager = new Thread(new DownloadManager(downloadUrl, catalogId), catalogId);
        downloadManager.run();
    }

    // Trust All Certifications
    private void doTrustAllCertificates() throws NoSuchAlgorithmException,
            KeyManagementException {
        TrustManager[] trustAllCerts = new TrustManager[] {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws
                            CertificateException {
                        return;
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws
                            CertificateException {
                        return;
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return null;
                    }
                }
        };

        // Set HttpsURLConnection settings
        SSLContext sslContext = SSLContext.getInstance("SSL");
        sslContext.init(null, trustAllCerts, new SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory());
        HostnameVerifier hostnameVerifier =
                (s, sslSession) -> s.equalsIgnoreCase(sslSession.getPeerHost());
        HttpsURLConnection.setDefaultHostnameVerifier(hostnameVerifier);
    }

}
