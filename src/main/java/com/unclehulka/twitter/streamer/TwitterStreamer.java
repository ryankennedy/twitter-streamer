package com.unclehulka.twitter.streamer;

import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.HttpResponse;

import java.io.InputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.ParseException;

import net.sf.json.JSONObject;

/**
 * A very simple implementation of a Twitter client utilizing the new Twitter stream APIs:
 *
 *     http://apiwiki.twitter.com/Streaming-API-Documentation
 *
 * The client connects to the stream API and parses the statuses returned, printing some simple
 * diagnostic information on each status update to the screen.
 *
 * This client makes use of Apache HttpComponents HttpClient and json-lib, which are available
 * at the URLs below.
 *
 *     http://hc.apache.org/httpcomponents-client/
 *     http://json-lib.sourceforge.net/
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
                System.out.println(String.format("Tweet from %s (%s)", status.getUser().getName(), status.getUser().getScreenName()));
                System.out.println(String.format("  %s", status.getText()));
                System.out.println("--------------------------------------------------");
            }
        }
        catch(IOException e) {
            // Handle errors. Disconnects, for example.
            System.err.println("Error connecting to spritzer: " + e.toString());
        }
    }

    /**
     * An Iterable TwitterStream, suitable for use in Java for-each loops.
     */
    private static class TwitterStream implements Iterable<TwitterStatus> {
        private TwitterStatusIterator iterator;

        public TwitterStream(InputStream stream) {
            iterator = new TwitterStatusIterator(stream);
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
        private BufferedReader reader;
        private TwitterStatus next;

        public TwitterStatusIterator(InputStream stream) {
            // Buffer the InputStream, the Twitter stream API returns statuses in JSON one per line
            // (newline delimited).
            reader = new BufferedReader(new InputStreamReader(stream));
            next = null;
        }

        public synchronized boolean hasNext() {
            // Check first to see if we haven't already read the next TwitterStatus from
            // the stream API.
            if(next == null) {
                try {
                    // No TwitterStatus held, try reading one.
                    JSONObject object = JSONObject.fromObject(readLine());
                    next = new TwitterStatus(object);
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

        private synchronized String readLine() throws IOException {
            // Synchronized access to the reader.
            return reader.readLine();
        }
    }

    /**
     * A simple TwitterStatus object to wrap the underlying JSONObject.
     */
    private static class TwitterStatus {
        private JSONObject status;
        private TwitterUser user;

        public TwitterStatus(JSONObject status) {
            this.status = status;
            user = new TwitterUser(status.getJSONObject("user"));
        }

        public String getText() {
            return status.getString("text");
        }

        public Date getCreatedAt() throws ParseException {
            SimpleDateFormat sdf = new SimpleDateFormat("E M d H:m:s Z y");
            return sdf.parse(status.getString("created_at"));
        }

        public String getSource() {
            return status.getString("source");
        }

        public TwitterUser getUser() {
            return user;
        }

        @Override
        public String toString() {
            return status.toString(2);
        }
    }

    /**
     * A simple TwitterUser object to wrap the underlying JSONObject.
     */
    private static class TwitterUser {
        private JSONObject user;

        public TwitterUser(JSONObject user) {
            this.user = user;
        }

        public String getUrl() {
            return user.getString("url");
        }

        public String getProfileImageUrl() {
            return user.getString("profile_image_url");
        }

        public String getScreenName() {
            return user.getString("screen_name");
        }

        public String getLocation() {
            return user.getString("location");
        }

        public String getName() {
            return user.getString("name");
        }

        public long getFriendCount() {
            return user.getLong("friends_count");
        }

        public long getStatusesCount() {
            return user.getLong("statuses_count");
        }

        public long getFavouritesCount() {
            return user.getLong("favourites_count");
        }

        public long getFollowersCount() {
            return user.getLong("followers_count");
        }

        public String getDescription() {
            return user.getString("description");
        }
    }
}