Twitter Streamer
================
A very simple implementation of a Twitter client utilizing the new Twitter stream APIs:

    https://dev.twitter.com/docs/streaming-api/methods

The client connects to the stream API and parses the statuses returned, printing some simple
diagnostic information on each status update to the screen.

To run the streamer invoke maven...

    mvn exec:java -Dexec.args="username password"
