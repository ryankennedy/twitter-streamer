package com.unclehulka.twitter.streamer;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.HttpResponse;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonAnySetter;
import org.codehaus.jackson.map.annotate.JsonDeserialize;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonFactory;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;

/**
 * A very simple implementation of a Twitter client utilizing the new Twitter stream APIs:
 *
 *     http://apiwiki.twitter.com/Streaming-API-Documentation
 *
 * The client connects to the stream API and parses the statuses returned, printing some simple
 * diagnostic information on each status update to the screen.
 *
 * This client makes use of Apache HttpComponents HttpClient and Jackson, which are available
 * at the URLs below.
 *
 *     http://hc.apache.org/httpcomponents-client/
 *     http://jackson.codehaus.org/
 *
 * @author Ryan Kennedy <ryan.kennedy@yahoo.com>
 */
public class TwitterStreamer {
    public static void main(String[] args) {
        // Set up an HttpClient instance.
        DefaultHttpClient client = new DefaultHttpClient();

        // Set up the credentials. Twitter's stream APIs require username/password.
        client.getCredentialsProvider().setCredentials(
                new AuthScope("stream.twitter.com", 80),
                new UsernamePasswordCredentials("username", "password"));

        // Call the "spritzer" stream, the others (gardenhose and firehose) require approval from Twitter.
        HttpGet get = new HttpGet("http://stream.twitter.com/spritzer.json");
        try {
            // Execute the request.
            HttpResponse response = client.execute(get);

            // Create a new TwitterStream using the InputStream from the HTTP connection.
            TwitterStream stream = new TwitterStream(response.getEntity().getContent());

            // Iterate over the TwitterStatus objects parsed from the stream.
            for(TwitterStatus status : stream) {
                // Dump out some simple information so we can see the tweets being fetched and parsed.
                System.out.println(String.format("Tweet from %s (%s)", status.user.name, status.user.screenName));
                System.out.println(String.format("  %s", status.text));
                System.out.println("--------------------------------------------------");
            }
        }
        catch(Exception e) {
            // Handle errors. Disconnects, for example.
            System.err.println("Error connecting to spritzer: " + e.toString());
        }
    }

    /**
     * An Iterable TwitterStream, suitable for use in Java for-each loops.
     */
    private static class TwitterStream implements Iterable<TwitterStatus> {
        private TwitterStatusIterator iterator;

        public TwitterStream(InputStream stream) throws Exception {
            iterator = new TwitterStatusIterator(new JsonFactory().createJsonParser(stream));
        }

        public Iterator<TwitterStatus> iterator() {
            return iterator;
        }
    }

    /**
     * A simple TwitterStatus iterator. Access to the iterator is synchronized to make sure two threads
     * don't bicker too much over the InputStream.
     */
    private static class TwitterStatusIterator implements Iterator<TwitterStatus> {
        private ObjectMapper mapper;
        private JsonParser parser;
        private TwitterStatus next;

        public TwitterStatusIterator(JsonParser parser) {
            mapper = new ObjectMapper();
            this.parser = parser;
            next = null;
        }

        public synchronized boolean hasNext() {
            // Check first to see if we haven't already read the next TwitterStatus from
            // the stream API.
            if(next == null) {
                try {
                    // No TwitterStatus held, try reading one.
                    next = mapper.readValue(parser, TwitterStatus.class);
                }
                catch(IOException e) {
                    // Failed to read one.
                    next = null;
                }
            }

            // If we've successfully held a TwitterStatus, then we have a next one.
            return next != null;
        }

        public synchronized TwitterStatus next() {
            // See if there's a next to return.
            if((next == null) && !hasNext()) {
                // None stored previously and an attempt to fetch a new one failed.
                throw new NoSuchElementException("No more statuses to return");
            }

            TwitterStatus toReturn = next;
            next = null;
            return toReturn;
        }

        public void remove() {
            // Remove isn't supported by this Iterator.
            throw new UnsupportedOperationException("TwitterStatusIterator doesn't support removal.");
        }
    }

    public static class TwitterStatus {
        @JsonProperty(value = "in_reply_to_user_id")
        public String inReplyToUserId;

        @JsonProperty(value = "favorited")
        public Boolean favorited;

        @JsonProperty(value = "in_reply_to_screen_name")
        public String inReplyToScreenName;

        @JsonProperty(value = "created_at")
        @JsonDeserialize(using = TwitterDateDeserializer.class)
        public Date createdAt;

        public String text;

        public TwitterUser user;

        public Long id;

        @JsonProperty(value = "in_reply_to_status_id")
        public Long inReplyToStatusId;

        public String source;

        @JsonAnySetter
        public void setProperty(String key, Object value) {
            // System.out.println(String.format("Missing @JsonProperty in TwitterStatus for %s", key));
        }
    }

    public static class TwitterUser {
        @JsonProperty(value = "profile_image_url")
        public String profileImageUrl;

        public Boolean verified;

        public String description;

        @JsonProperty(value = "screen_name")
        public String screenName;

        @JsonProperty(value = "followers_count")
        public Long followersCount;

        public String name;

        @JsonProperty(value = "created_at")
        @JsonDeserialize(using = TwitterDateDeserializer.class)
        public Date createdAt;

        @JsonProperty(value = "friends_count")
        public Long friendsCount;

        @JsonProperty(value = "statuses_count")
        public Long statusesCount;

        @JsonProperty(value = "favourites_count")
        public Long favouritesCount;

        public String url;

        public Long id;

        public String location;

        @JsonAnySetter
        public void setProperty(String key, Object value) {
            // System.out.println(String.format("Missing @JsonProperty in TwitterUser for %s", key));
        }
    }

    public static class TwitterDateDeserializer extends JsonDeserializer<Date> {
        public Date deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
                return sdf.parse(jp.getText());
            }
            catch(ParseException e) {
                throw new IOException("Error parsing Twitter date format: " + e.toString());
            }
        }
    }
}