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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.cometd.bayeux.client.ClientSessionChannel.MessageListener;
import org.cometd.client.BayeuxClient;

/**
 * This class prepares a query for the DDF Cometd Search Endpoint.
 * This query is possibly a long running operation and should be run inside of a separate thread.
 */
public class AsyncQuery implements Runnable {

    private static final String QUERY_SERVICE = "/service/query";

    private final BayeuxClient bayeuxClient;
    private final String keyword;
    private final Map<String, Object> queryResponse;
    private final String id;
    private final String responseChannel;

    /**
     * Prepares a query for the DDF Cometd Search Endpoint
     * @param bayeuxClient - reference to the {@link BayeuxClient} used by this session.
     * @param keyword - keyword to be used for the query.
     * @param queryResponse - reference to a {@link Map} for storing the query response.
     */
    public AsyncQuery(BayeuxClient bayeuxClient, String keyword, Map<String, Object> queryResponse) {
        this.bayeuxClient = bayeuxClient;
        this.keyword = keyword;
        this.queryResponse = queryResponse;

        this.id = UUID.randomUUID().toString();
        this.responseChannel = "/" + id;
    }

    @Override
    public void run() {
        Map<String, Object> request = new HashMap<>();
        request.put("id", id);
        request.put("cql", "anyText ILIKE  '" + keyword + "'");
        bayeuxClient.getChannel(responseChannel).addListener((MessageListener)
                (channel, message) -> queryResponse.putAll(message.getDataAsMap()));

        bayeuxClient.getChannel(QUERY_SERVICE).publish(request);
    }

    public void destroy() {
        bayeuxClient.getChannel(responseChannel).unsubscribe();
    }
}
