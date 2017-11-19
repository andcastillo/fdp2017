package org.db;

import java.util.Enumeration;

import org.db.core.Indexer;
import org.db.scan.IndexScan;

public class IndexScanTest {
    public static void main(String[] args){
        // Hash table test
        System.out.println("------- Hash indexing test (table: A)-------");
        Indexer indexer = new Indexer();
        indexer.indexTable("A");
        IndexScan aIndexScan = new IndexScan("A", "b", indexer);
        while (aIndexScan.hasNext()) {
            System.out.println(aIndexScan.next());
        };
        // Btree test
        System.out.println("\n------- BTree indexing test (table: B) -------");
        indexer.indexTable("B");

        System.out.println("\n------- Indexing when column is not indexed test (table: B, column:x) -------");
        // Test when column is not indexed
        IndexScan bIndexScan = new IndexScan("B", "x", indexer);
        while (bIndexScan.hasNext()) {
            System.out.println(bIndexScan.next());
        };
    }
}
