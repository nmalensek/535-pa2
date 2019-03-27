package parallel;

import java.io.Serializable;

public class BucketEntry implements Serializable, Comparable<BucketEntry> {
    private String hashtag;
    private int count;
    private int delta;

    public BucketEntry(String hashtag, int count, int delta) {
        this.hashtag = hashtag;
        this.count = count;
        this.delta = delta;
    }

    @Override
    public int compareTo(BucketEntry other) {
        long diff = this.getCount() - other.getCount();
        if (diff > 0) {
            return 1;
        } else if (diff < 0) {
            return -1;
        } else {
            return 0;
        }
    }

    public void incrementCount() {
        count++;
    }

    public String getHashtag() {
        return hashtag;
    }

    public int getCount() {
        return count;
    }

    public int getDelta() {
        return delta;
    }

    public int getFrequencyPlusDelta() {
        return count + delta;
    }

    @Override
    public String toString() {
        return hashtag + "|" + count + "|" + delta;
    }
}
