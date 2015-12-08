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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DownloadManager implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncClient.class);

    private URL url;
    private String outputFileName;

    public DownloadManager(String url, String outputFileName) throws MalformedURLException {
        this.url = new URL(url);
        this.outputFileName = outputFileName;
    }

    public void run() {
        ReadableByteChannel rbc = null;
        try {
            rbc = Channels.newChannel(url.openStream());
        } catch (IOException e) {
            LOGGER.error("Error opening stream from url: " + url);
            LOGGER.error(e.getMessage());
        }
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(outputFileName);
        } catch (FileNotFoundException e) {
            LOGGER.error("Could not find file: " + outputFileName);
            LOGGER.error(e.getMessage());
        }
        try {
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        } catch (IOException e) {
            LOGGER.error("Error downloading file");
            LOGGER.error(e.getMessage());
        }
    }
}
