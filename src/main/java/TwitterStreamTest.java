import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TwitterStreamTest {

    public static void main(String[] args) {
        TwitterStream twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build()).getInstance()
                .addListener(new StatusListener() {
                    public void onStatus(Status status) {
                        if (status.getHashtagEntities().length == 0) {
                            return;
                        }
                        List<String> tagStrings = Arrays.stream(status.getHashtagEntities()).map(HashtagEntity::getText).collect(Collectors.toList());
                        String[] tagArray = Arrays.stream(status.getHashtagEntities()).map(HashtagEntity::getText).toArray(String[]::new);
                        System.out.println(String.join(" ", tagArray));
//                        for (HashtagEntity tag : status.getHashtagEntities()) {
//                            System.out.print("#" + tag.getText() + " ");
//                        }
                        System.out.println();
                    }

                    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                    }

                    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                        System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
                    }

                    public void onScrubGeo(long userId, long upToStatusId) {
                        System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
                    }

                    public void onStallWarning(StallWarning warning) {
                        System.out.println("Got stall warning:" + warning);
                    }

                    public void onException(Exception ex) {
                        ex.printStackTrace();
                    }
                }).sample();
    }
}
