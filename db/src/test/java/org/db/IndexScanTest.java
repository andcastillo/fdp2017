package db.src.test.java.org.db;

import java.util.Enumeration;

public class IndexScanTest {
    public static void main(String[] args){
        // Hash table test
        Indexer indexer = new Indexer();
        indexer.indexTable("A");
        IndexScan aIndexScan = new IndexScan("A", "b", indexer);
        while (aIndexScan.hasNext()) {
            System.out.println(aIndexScan.next());
        };
        // Btree test
        indexer.indexTable("B");
    }
}
