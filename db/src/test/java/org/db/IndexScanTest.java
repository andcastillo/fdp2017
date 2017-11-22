package org.db;

import java.util.Enumeration;

import org.db.core.Indexer;
import org.db.scan.IndexScan;

public class IndexScanTest {
    public static void main(String[] args){
        Indexer indexer = new Indexer();
        // Hash table test
        System.out.println("----------------------------------");
        System.out.println("------- Hash indexing test -------");
        System.out.println("----------------------------------");
        System.out.println("\n------- Hash indexing test (table: A) index according to schema-------");
        indexer.indexTable("A");
        IndexScan aIndexScan = new IndexScan("A", "b", indexer);
        while (aIndexScan.hasNext()) {
            System.out.println(aIndexScan.next());
        };System.out.println("\n------- Hash indexing test (table: A) = \"def\"-------");
        indexer.indexTable("A");
        aIndexScan = new IndexScan("A", "b", indexer, "=", "\"def\"");
        while (aIndexScan.hasNext()) {
            System.out.println(aIndexScan.next());
        };


        System.out.println("\n\n----------------------------------");
        System.out.println("------- Tree indexing test -------");
        System.out.println("----------------------------------");
        // Tree test
        System.out.println("\n------- Tree indexing test (table: B) index according to schema -------");
        indexer.indexTable("B");
        IndexScan bIndexScan = new IndexScan("B", "A", indexer);
        while (bIndexScan.hasNext()) {
            System.out.println(bIndexScan.next());
        };

        System.out.println("\n------- Tree indexing test (table: B) = 2 -------");
        indexer.indexTable("B");
        bIndexScan = new IndexScan("B", "A", indexer, "=", 2);
        while (bIndexScan.hasNext()) {
            System.out.println(bIndexScan.next());
        };

        // Tree test
        System.out.println("\n------- Tree indexing test (table: B) > 3 -------");
        indexer.indexTable("B");
        bIndexScan = new IndexScan("B", "A", indexer, ">", 3);
        while (bIndexScan.hasNext()) {
            System.out.println(bIndexScan.next());
        };

        System.out.println("\n------- Tree indexing test (table: B) >= 6 -------");
        indexer.indexTable("B");
        bIndexScan = new IndexScan("B", "A", indexer, ">=", 6);
        while (bIndexScan.hasNext()) {
            System.out.println(bIndexScan.next());
        };

        // Tree test
        System.out.println("\n------- Tree indexing test (table: B) < 6 -------");
        indexer.indexTable("B");
        bIndexScan = new IndexScan("B", "A", indexer, "<", 6);
        while (bIndexScan.hasNext()) {
            System.out.println(bIndexScan.next());
        };

        System.out.println("\n------- Tree indexing test (table: B) <= 5 -------");
        indexer.indexTable("B");
        bIndexScan = new IndexScan("B", "A", indexer, "<=", 5);
        while (bIndexScan.hasNext()) {
            System.out.println(bIndexScan.next());
        };

        System.out.println("\n------- Indexing when column is not indexed test (table: B, indexColumn:x 3rd) -------");
        // Test when column is not indexed
        bIndexScan = new IndexScan("B", "x", indexer);
        while (bIndexScan.hasNext()) {
            System.out.println(bIndexScan.next());
        };

        System.out.println("\n------- Indexing when column is not indexed test (table: B, indexColumn:price 4th, >5.0) -------");
        // Test when column is not indexed
        bIndexScan = new IndexScan("B", "price", indexer, ">", 5.0);
        while (bIndexScan.hasNext()) {
            System.out.println(bIndexScan.next());
        };
    }
}
