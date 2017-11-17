import java.util.Enumeration;

public class Main {
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
