import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Enumeration;

public class IndexScan implements Iterator<List<Object>>{


    private static final String DEFAULT_SEPARATOR = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

	private String tableName; 		//Nombre de la tabla (Directorio) a escanear
	private int limit = 10;			//Limite del tama�o de cada bloque
	private int countBlocks = 1;	//Contador de bloques
	private int countRows = 1;		//Contador de registros dentro de cada bloque
	private String dataSchema; 		//Contiene la informacion del esquema a escanear
	private Indexer indexer;
	private Enumeration<Object> hashKeys;

	public IndexScan(Indexer indexer) {
		this.tableName = indexer.tableName;
		this.indexer = indexer;
		// Hash table
		hashKeys = indexer.indexedHashtable.keys();
		loadSchema();
	}

	//Se pregunta si hay siguiente en un bloque
	//De lo contrario se carga el siguiente bloque y se inicia nuevamente el contador de registros
	@Override
	public boolean hasNext() {
		return this.hashKeys.hasMoreElements();
	}

	@Override
	public List<Object> next() {
		//For hash table
		if (this.hashKeys.hasMoreElements()) {
			Object key = this.hashKeys.nextElement();
			Object blockNumber = indexer.indexedHashtable.get(key);
			return findRowInBlock((Integer) blockNumber, key);
		}
		return null;
	}

	private List<Object> findRowInBlock (Integer blockNumber, Object index) {
		Scanner block = openBlock(blockNumber);
		List<Object> row;
		while (block.hasNext()){
			 row = parseRow(block.nextLine(), dataSchema);
			 if (row.get(indexer.hashIndex).equals(index)) {
			 	return row;
			 }
		}
		return null;
	}

	private Scanner openBlock(Integer blockNumber){
		File file = new File("data/myDB/"+this.tableName+"/"+blockNumber+".csv");
		Scanner inputStream;
		try {
			inputStream = new Scanner(file);
		} catch (FileNotFoundException e) {
			//e.printStackTrace();
			//No se encuentra bloque
			return null;
		}
		return inputStream;
	}

	private void loadSchema (){
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

	public List<Object> parseRow(String row , String schema){
		String[] tempRow = row.split(DEFAULT_SEPARATOR);
		String[] tempSchema = schema.split(DEFAULT_SEPARATOR);
		List<Object> list = null;
		if(tempRow.length == tempSchema.length){
			list = new ArrayList<>();
			for (int i = 0; i < tempRow.length; i++) {
				String[] schemaCol = tempSchema[i].split("-");
				String data = tempRow[i];
				if(schemaCol[1].equals("Integer")){
					list.add(Integer.parseInt(data));
				}else if (schemaCol[1].equals("Double")){
					list.add(Double.parseDouble(data));
				}else if (schemaCol[1].equals("Boolean")){
					list.add(Boolean.parseBoolean(data));
				}else {//String
					list.add(String.valueOf(data));
				}			
			}
		}
		countRows++;
		return list;
	}
}

