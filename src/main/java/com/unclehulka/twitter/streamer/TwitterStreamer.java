package com.unclehulka.twitter.streamer;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import org.codehaus.jackson.*;
import org.codehaus.jackson.annotate.JsonAnySetter;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.DeserializationContext;
import org.codehaus.jackson.map.JsonDeserializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.PropertyNamingStrategy;
import org.codehaus.jackson.map.annotate.JsonDeserialize;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

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
        stream(args[0], args[1]);
    }

    private static void stream(String username, String password) {
        // Set up a Client instance.
        Client client = new ApacheHttpClient4();
        client.addFilter(new HTTPBasicAuthFilter(username, password));
        
        // Set up the credentials. Twitter's stream APIs require username/password.
        WebResource resource = client.resource("https://stream.twitter.com/1/statuses/sample.json");

        // Call the "sample" stream, the heavier hoses require approval from Twitter.
        try {
            // Execute the request.
            ClientResponse response = resource.get(ClientResponse.class);
            
            if(response.getStatus() != 200) {
                System.err.println(response.getEntity(String.class));
                return;
            }

            // Create a new TwitterStream using the InputStream from the HTTP connection.
            TwitterStream stream = new TwitterStream(response.getEntityInputStream());

            // Iterate over the TwitterStatus objects parsed from the stream.
            for(TwitterStatus status : stream) {
                // Dump out some simple information so we can see the tweets being fetched and parsed.
                if(status.user != null) {
                    System.out.println(String.format("Tweet from %s (%s)", status.user.name, status.user.screenName));
                    System.out.println(String.format("  %s", status.text));
                    System.out.println("--------------------------------------------------");
                }
                else if(status.delete != null) {
                    System.out.println(String.format("Tweet %s deleted by %s", status.delete.status.idStr, status.delete.status.userIdStr));
                    System.out.println("--------------------------------------------------");
                }
                else {
                    // Catch all for objects we don't fully understand. Twitter is good at adding new types without
                    // changing the API version.
                    System.err.println("Not sure what this object is...");
                    JsonGenerator generator = new JsonFactory().createJsonGenerator(System.err, JsonEncoding.UTF8);
                    generator.setCodec(new ObjectMapper());
                    generator.writeObject(status);
                }
            }

            // The stream iterator
            System.out.println("Disconnected");
        }
        catch(IOException e) {
            // Handle errors.
            System.err.println("Error processing Twitter stream: " + e.toString());
            e.printStackTrace(System.err);
        }
    }

    /**
     * An Iterable TwitterStream, suitable for use in Java for-each loops.
     */
    private static class TwitterStream implements Iterable<TwitterStatus> {
        private TwitterStatusIterator iterator;

        public TwitterStream(InputStream stream) throws IOException {
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
        private BlockingQueue<JsonNode> statusQueue;
        private ObjectMapper mapper;
        private boolean connected;

        public TwitterStatusIterator(final JsonParser parser) {
            // Create a queue to hold the JSON objects parsed from the stream.
            statusQueue = new ArrayBlockingQueue<JsonNode>(50, false);

            mapper = new ObjectMapper();
            mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

            // Whether or not the connection to Twitter is still open.
            connected = true;

            // Parse the JSON object skeleton in a separate thread to make sure we're keeping up with
            // the stream from Twitter. Not keeping up will cause Twitter to disconnect from their end.
            Thread reader = new Thread(new Runnable() {
                public void run() {
                    while(true) {
                        try {
                            // Try putting the JSON object in the queue if it will fit.
                            if(!statusQueue.offer(mapper.readTree(parser))) {
                                System.err.println("Dropped status");
                            }
                        }
                        catch(JsonProcessingException e) {
                            // A badly formatted JSON object, perhaps. Skip it.
                            System.err.println("Failed to parse JSON object: " + e.toString());
                            e.printStackTrace(System.err);
                        }
                        catch(IOException e) {
                            // An IOException may indicate that Twitter has closed the stream on their end.
                            System.err.println("Failed to parse JSON object: " + e.toString());
                            e.printStackTrace(System.err);
                            connected = false;
                            return;
                        }
                    }
                }
            });
            reader.start();
        }

        public synchronized boolean hasNext() {
            return connected;
        }

        public synchronized TwitterStatus next() {
            // See if there's a next to return.
            while(hasNext()) {
                // Try to parse and return a status object. Discard and log bogus JSON objects.
                try {
                    JsonNode node = statusQueue.take();
                    try {
                        return mapper.treeToValue(node, TwitterStatus.class);
                    } catch (Exception e) {
                        System.err.println("Failed to successfully parse JsonNode: " + e.toString());
                        System.err.println(node);
                        e.printStackTrace(System.err);
                    }
                }
                catch(InterruptedException e) {
                    System.err.println("Interrupted while waiting to process queue");
                    e.printStackTrace(System.err);
                    connected = false;
                }
            }

            throw new NoSuchElementException("No more statuses to return");
        }

        public void remove() {
            // Remove isn't supported by this Iterator.
            throw new UnsupportedOperationException("TwitterStatusIterator doesn't support removal.");
        }
    }


    public static class TwitterStatus {
        public Long id;

        public String idStr;

        public String text;

        public Boolean retweeted;

        public String inReplyToStatusIdStr;

        @JsonDeserialize(using = TwitterDateDeserializer.class)
        public Date createdAt;

        public Long inReplyToStatusId;

        public String inReplyToUserIdStr;

        public Long inReplyToUserId;
        
        public TwitterDelete delete;
        
        public TwitterCoordinates coordinates;
        
        public String source;

        public Boolean favorited;

        public Long retweetCount;
        
        public String inReplyToScreenName;

        public Boolean truncated;
        
        public TwitterEntities entities;

        public Hashtags hashtags;
        
        public List<Long> contributors;

        public JsonNode place;
        
        public JsonNode geo;

        public TwitterUser user;

        public RetweetedStatus retweetedStatus;

        public Boolean possiblySensitive;

        public Boolean possiblySensitiveEditable;
        
        public JsonNode urls;

        @JsonAnySetter
        public void setProperty(String key, Object value) {
            System.err.println(String.format("Missing @JsonProperty in TwitterStatus for %s => %s", key, value));
        }
    }

    public static class TwitterUser {
        public String idStr;

        public Boolean contributorsEnabled;

        public Boolean verified;
        
        public JsonNode notifications;

        public Boolean profileUseBackgroundImage;
        
        public String profileTextColor;

        public Boolean defaultProfileImage;
        
        public String profileBackgroundImageUrl;

        public String profileLinkColor;

        public String profileBackgroundColor;

        public Boolean profileBackgroundTile;
        
        public String profileSidebarBorderColor;
        
        public String profileBackgroundImageUrlHttps;
        
        public String profileImageUrl;

        public String profileImageUrlHttps;

        public String profileSidebarFillColor;
        
        public String description;

        public Boolean showAllInlineMedia;

        public int favouritesCount;
                
        public String url;
        
        public String lang;

        public Long statusesCount;
        
        public String timeZone;

        public Boolean geoEnabled;

        @JsonProperty("protected")
        public Boolean isProtected;

        public Long listedCount;

        public JsonNode location;
        
        public String screenName;
        
        public String name;

        public JsonNode following;

        public Long friendsCount;

        public Long id;

        public Boolean defaultProfile;

        public JsonNode followRequestSent;

        public Boolean isTranslator;

        public Long utcOffset;

        public Long followersCount;

        @JsonDeserialize(using = TwitterDateDeserializer.class)
        public Date createdAt;

        @JsonAnySetter
        public void setProperty(String key, Object value) {
            System.err.println(String.format("Missing @JsonProperty in TwitterUser for %s => %s", key, value));
        }
    }

    public static class TwitterDelete {
        public TwitterDeletedStatus status;

        @JsonAnySetter
        public void setProperty(String key, Object value) {
            System.err.println(String.format("Missing @JsonProperty in TwitterDelete for %s => %s", key, value));
        }
    }

    public static class TwitterDeletedStatus {
        public Long id;
        
        public String idStr;
        
        public Long userId;
        
        public String userIdStr;

        @JsonAnySetter
        public void setProperty(String key, Object value) {
            System.err.println(String.format("Missing @JsonProperty in TwitterDeletedStatus for %s => %s", key, value));
        }
    }
    
    public static class TwitterCoordinates {
        public String type;

        public List<Double> coordinates;

        @JsonAnySetter
        public void setProperty(String key, Object value) {
            System.err.println(String.format("Missing @JsonProperty in TwitterCoordinates for %s => %s", key, value));
        }
    }
    
    public static class TwitterEntities {
        public JsonNode userMentions;
        
        public JsonNode hashtags;
        
        public JsonNode media;
        
        public JsonNode urls;
        
        public JsonNode displayUrl;

        public JsonNode expandedUrl;
        
        public JsonNode url;

        @JsonAnySetter
        public void setProperty(String key, Object value) {
            System.err.println(String.format("Missing @JsonProperty in TwitterEntities for %s => %s", key, value));
        }
    }
    
    public static class UserMention {
        public String idStr;
        
        public List<Integer> indices;
        
        public String name;
        
        public String screenName;
        
        public Long id;

        @JsonAnySetter
        public void setProperty(String key, Object value) {
            System.err.println(String.format("Missing @JsonProperty in UserMention for %s => %s", key, value));
        }
    }

    public static class Media {
        public String idStr;
        
        public String type;
        
        public List<Integer> indices;

        public String displayUrl;
        
        public String mediaUrl;
        
        public String url;
        
        public String expandedUrl;
        
        public String mediaUrlHttps;

        public Long id;
        
        public Map<String, MediaSize> sizes;

        @JsonAnySetter
        public void setProperty(String key, Object value) {
            System.err.println(String.format("Missing @JsonProperty in Media for %s => %s", key, value));
        }
    }

    public static class MediaSize {
        public String resize;

        public Long h;

        public Long w;
        
        @JsonAnySetter
        public void setProperty(String key, Object value) {
            System.err.println(String.format("Missing @JsonProperty in MediaSize for %s => %s", key, value));
        }
    }
    
    public static class Hashtags {
        public String url;
        
        public List<String> urls;

        public JsonNode userMentions;
        
        public String expandedUrl;
        
        public String displayUrl;
    }

    public static class RetweetedStatus {
        public String idStr;
        
        public String text;

        public JsonNode geo;

        public Boolean favorited;

        public Boolean retweeted;
        
        public String source;
        
        public Long retweetCount;
        
        public String inReplyToScreenName;

        public JsonNode entities;
        
        public Long inReplyToStatusId;

        public String inReplyToStatusIdStr;

        public Long inReplyToUserId;

        public String inReplyToUserIdStr;

        @JsonDeserialize(using = TwitterDateDeserializer.class)
        public Date createdAt;

        public JsonNode contributors;

        public JsonNode place;

        public JsonNode coordinates;

        public TwitterUser user;

        public Boolean truncated;

        public Long id;

        public Boolean possiblySensitive;

        public Boolean possiblySensitiveEditable;

        @JsonAnySetter
        public void setProperty(String key, Object value) {
            System.err.println(String.format("Missing @JsonProperty in RetweetedStatus for %s => %s", key, value));
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