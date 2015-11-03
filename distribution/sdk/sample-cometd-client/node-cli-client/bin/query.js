#!/usr/bin/env node
/*
use strict
*/
var fs    = require('fs'),
    faye  = require('faye'),
    uuid  = require('node-uuid');

var endpoint      = 'http://localhost:8181/cometd',
    notifications = '/ddf/notifications/**',
    query         = '/service/query',
    activities    = '/ddf/activities/**',
    guid          = uuid.v4(),
    guidChannel   = '/' + guid,
    keyword = process.argv[2] || '*',
    results;

console.info('Starting faye client at: ' + endpoint)
var client = new faye.Client(endpoint);
client.disable('websocket');

client.on('transport:down', function() {
  console.error('Cometd Connection failure');
});

client.on('transport:up', function() {
  console.info('Cometd connection active');
});

console.info('Subscribing to query response: ' + guidChannel);
client.subscribe(guidChannel, function(message) {
   this.results = message;
  console.info('Query Results: ' + JSON.stringify(message["status"][0]["results"]))
  for (i = 0; i < message["results"].length; i++) {
    result = message["results"][i];
    metacard = result["metacard"];
    isCached = metacard["cached"];
    console.info('Result #' + i);
    console.info('Title: ' + JSON.stringify(result["metacard"]["properties"]["title"]));
    if(isCached) {
      console.info('Cached On: ' + isCached);
    }
  }
});

queryMessage = {
                  "id":guid,
                  "cql":"anyText LIKE \'" + keyword + "\'"
                };

console.info('Publishing query: ' + JSON.stringify(queryMessage));
setTimeout(function() { client.publish(query, queryMessage) }, 500);
