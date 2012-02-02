Twitter Streamer
================
A very simple implementation of a Twitter client utilizing the new Twitter stream APIs:

    http://apiwiki.twitter.com/Streaming-API-Documentation

The client connects to the stream API and parses the statuses returned, printing some simple
diagnostic information on each status update to the screen.

This client makes use of Apache HttpComponents HttpClient and json-lib, which are available
at the URLs below.

    http://hc.apache.org/httpcomponents-client/
    http://json-lib.sourceforge.net/
