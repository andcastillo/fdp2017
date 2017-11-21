package org.db.scan;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import org.db.core.Indexer;

public class IndexScan implements Iterator<List<Object>>{


	private static final String DEFAULT_SEPARATOR = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

	private String tableName; 		//Nombre de la tabla (Directorio) a escanear
	private String columnName;
	private String dataSchema; 		//Contiene la informacion del esquema a escanear
	private Indexer indexer;
	private Iterator<String> blockLines;
	private String indexMethod;

	public IndexScan(String tableName, String columnName, Indexer indexer) {
		this.tableName = tableName;
		this.columnName = columnName;
		this.indexer = indexer;
		if (indexer.columnIsHashIndexed(tableName, columnName)) {
			indexMethod = "hash";
		} else if (indexer.columnIsBTreeIndexed(tableName, columnName)) {
			indexMethod = "btree";
		} else {
			indexMethod = indexer.indexColumn(tableName, columnName);
		}
		if (indexMethod.equals("hash")) {
			blockLines = Indexer.hashIndexes.get(tableName).get(columnName).values().iterator();
		} else {
			Indexer.btreeIndexes.get(tableName).get(columnName).restartScan();
		}
		loadSchema();
	}

	//Se pregunta si hay siguiente en un bloque
	//De lo contrario se carga el siguiente bloque y se inicia nuevamente el contador de registros
	@Override
	public boolean hasNext() {
		if (this.indexMethod.equals("hash")) {
			return this.blockLines.hasNext();
		} else {
			return Indexer.btreeIndexes.get(tableName).get(columnName).hasNext();
		}
	}

	@Override
	public List<Object> next() {
		String blockLine = "";
		if (this.indexMethod.equals("hash")) {
			//For hash table
			if (this.blockLines.hasNext()) {
				blockLine = this.blockLines.next();
			}
		} else {
			if (Indexer.btreeIndexes.get(tableName).get(columnName).hasNext()) {
				blockLine = (String) Indexer.btreeIndexes.get(tableName).get(columnName).next();
			}
		}
		return findRowInBlock(blockLine);
	}


	private List<Object> findRowInBlock (String blockLine) {
		String [] splitBlockLine = blockLine.split(",");
		Integer blockNumber = Integer.parseInt(splitBlockLine[0]);
		Integer lineNumber = Integer.parseInt(splitBlockLine[1]);
		Scanner block = openBlock(blockNumber);
		for (int i = 0; i<= lineNumber; i++) {
			String rowAsString = block.nextLine();
			if (lineNumber == i) {
				block.close();
				return parseRow(rowAsString);
			}
		}
		block.close();
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

	private List<Object> parseRow(String row){
		String[] tempRow = row.split(DEFAULT_SEPARATOR);
		String[] tempSchema = dataSchema.split(DEFAULT_SEPARATOR);
		List<Object> list = null;
		if(tempRow.length == tempSchema.length){
			list = new ArrayList<Object>();
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
		return list;
	}
}