package util;

import parallel.BucketEntry;

import java.util.*;

public class DataStructureTesting {

    private int delta = 3;

    private BucketEntry[] orderingTest = {
            new BucketEntry("apple", 1, 2),
            new BucketEntry("orange", 2, 3),
            new BucketEntry("pear", 5, 4),
            new BucketEntry("banana", 1, 1),
            new BucketEntry("pineapple", 3, 1),
    };

    private HashMap<String, BucketEntry> items = new HashMap<>();

    public HashMap<String, BucketEntry> getItems() {
        return items;
    }

    private List<BucketEntry> getSortedRankings() {
        List<BucketEntry> entries = new ArrayList<>(Arrays.asList(orderingTest));
        Collections.sort(entries);
        Collections.reverse(entries);

        return entries;
    }

    private void testDeletion() {
        for (BucketEntry entry : orderingTest) {
            items.put(entry.getHashtag(), entry);
        }

        items.entrySet().removeIf(e -> e.getValue().getFrequencyPlusDelta() <= this.delta);
    }

    private void testUpdate() {
        for (BucketEntry entry : orderingTest) {
            items.put(entry.getHashtag(), entry);
        }

        items.merge("apple", new BucketEntry("apple", 1, this.delta - 1), (pVal, nVal) -> {
           pVal.incrementCount();
           return pVal;
        });

        items.merge("apricot", new BucketEntry("apricot", 1, this.delta - 1), (pVal, nVal) -> {
            pVal.incrementCount();
            return pVal;
        });
    }

    public static void main(String[] args) {
        DataStructureTesting dataStructureTesting = new DataStructureTesting();
        System.out.println(dataStructureTesting.getSortedRankings());
        dataStructureTesting.testUpdate();
        System.out.println(dataStructureTesting.getItems());
    }
}
