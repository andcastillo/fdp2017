import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.Hashtable;

public class Indexer {
    private static final String DEFAULT_SEPARATOR = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
    private String dataSchema; 		//Contiene la informacion del esquema a escanea
    private int countBlocks = 1;	//Contador de bloques
    private int countRows = 1;		//Contador de registros dentro de cada bloque
    private int limit = 10;			//Limite del tama�o de cada bloque
    private Scanner inputStream;


    public String tableName; 		//Nombre de la tabla (Directorio) a escanear
    public Integer hashIndex;
    public Hashtable<Object, Object> indexedHashtable = new Hashtable<Object, Object>();
    public BTree btree = new BTree();

    public Indexer (String tableName) {
        this.tableName = tableName;
    }

    public void indexTable() {
        readSchema();
        String[] tempSchema = dataSchema.split(DEFAULT_SEPARATOR);
        for (int i = 0; i < tempSchema.length; i++) {
            String[] schemaCol = tempSchema[i].split("-");
            String data = tempSchema[i];
            if(schemaCol.length == 3){
                if (schemaCol[2].equals("hash")) {
                    fillHashTable(i);
                    // TODO: Check if more than one index per table is possible
                    this.hashIndex = i;
                }
                if (schemaCol[2].equals("btree")) {
                    fillBTree(i);
                }
            }
        }
    }

    private void readSchema(){
        this.tableName = tableName;
        File file = new File("data/myDB/"+this.tableName+"/schema.txt"); //Se carga el esquema
        try {
            Scanner inputStream = new Scanner(file);
            while (inputStream.hasNext()) {
                dataSchema = inputStream.nextLine();
            }
            inputStream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void fillHashTable(int columnIndex) {
        countBlocks = 1;
        inputStream = nextBlock();
        do {
            countRows = 0;
            while (countRows < limit && inputStream.hasNext()) {
                String row = inputStream.nextLine();
                String[] tempRow = row.split(DEFAULT_SEPARATOR);
                indexedHashtable.put(tempRow[columnIndex], countBlocks - 1);
                countRows++;
            }
            inputStream = nextBlock();
        } while (inputStream != null);
        this.close();
    }

    private void fillBTree (int columnIndex) {
        countBlocks = 1;
        inputStream = nextBlock();
        do {
            countRows = 0;
            while (countRows < limit && inputStream.hasNext()) {
                String row = inputStream.nextLine();
                String[] tempRow = row.split(DEFAULT_SEPARATOR);
                btree.put(tempRow[columnIndex], row);
                countRows++;
            }
            inputStream = nextBlock();
        } while (inputStream != null);
        this.close();
    }

    private Scanner nextBlock(){
        File file = new File("data/myDB/"+tableName+"/"+countBlocks+".csv");
        try {
            inputStream = new Scanner(file);
        } catch (FileNotFoundException e) {
            //e.printStackTrace();
            //No se encuentran mas bloques
            return null;
        }
        countBlocks++;
        return inputStream;
    }

    public void close(){
        if ( inputStream != null ) {
            inputStream.close();
        }
    }
}
