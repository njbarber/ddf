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
package org.codice.ddf.cli.commands;

import javax.inject.Inject;

import org.codice.ddf.cli.RunnableCommand;
import org.codice.ddf.cli.modules.GlobalOptions;
import org.codice.ddf.cli.ui.Notify;
import org.codice.ddf.sdk.cometd.AsyncClient;

import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;

@Command(name = "query", description = "Search ddf catalog")
public class Query implements RunnableCommand {

    @Inject
    private GlobalOptions globtions = new GlobalOptions();

    @Option(name = "--cid-only", description = "When set, results will only display the catalog id's")
    private boolean cidOnly = false;

    @Arguments(description = "Keyword for query, defaults to %")
    String keyword = "%";

    private AsyncClient asyncClient;

    @Override
    public int run() {
        String url = globtions.getUrl();
        new Thread() {
            public void run() {
                try {
                    asyncClient = new AsyncClient(url, true);
                    asyncClient.query(keyword);
                } catch (Exception e) {
                    Notify.error("Client Error", "Client did not start correctly", e.toString());
                    return;
                }
            }
        }.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Notify.error("Error performing query", e.getMessage());
            return 1;
        }
        Notify.normal("Query Results: " + asyncClient.getQueryResponse().size(), null, asyncClient.getQueryResponse().toString());

        return 0;
    }
}
