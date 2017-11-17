package org.db.scan;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.db.core.Indexer;

import java.util.Enumeration;

public class IndexScan implements Iterator<List<Object>>{


	private static final String DEFAULT_SEPARATOR = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

	private String tableName; 		//Nombre de la tabla (Directorio) a escanear
	private String columnName;
	private int limit = 10;			//Limite del tama�o de cada bloque
	private int countBlocks = 1;	//Contador de bloques
	private int countRows = 1;		//Contador de registros dentro de cada bloque
	private String dataSchema; 		//Contiene la informacion del esquema a escanear
	private Indexer indexer;
	private Enumeration<String> hashColumnValues;
	private String indexMethod;

	public IndexScan(String tableName, String columnName, Indexer indexer) {
		this.tableName = tableName;
		this.columnName = columnName;
		if (indexer.columnIsHashIndexed(tableName, columnName)) {
			indexMethod = "hash";
			hashColumnValues = Indexer.hashIndexes.get(tableName).get(columnName).keys();
		} else if (indexer.columnIsBTreeIndexed(tableName, columnName)) {
			indexMethod = "btree";
		} else {
			// TODO: indexColumn
		}
		loadSchema();
	}

	//Se pregunta si hay siguiente en un bloque
	//De lo contrario se carga el siguiente bloque y se inicia nuevamente el contador de registros
	@Override
	public boolean hasNext() {
		return this.hashColumnValues.hasMoreElements();
	}

	@Override
	public List<Object> next() {
		if (this.indexMethod.equals("hash")) {
			//For hash table
			if (this.hashColumnValues.hasMoreElements()) {
				String columnValue = this.hashColumnValues.nextElement();
				Integer blockNumber = indexer.hashIndexes.get(tableName).get(this.columnName).get(columnValue);
				return findRowInBlock(blockNumber, columnValue);
			}
		} else {
			//TODO: next for btree
		}
		return null;
	}

	private List<Object> findRowInBlock (Integer blockNumber, String columnValue) {
		Scanner block = openBlock(blockNumber);
		List<Object> row;
		while (block.hasNext()){
			row = parseRow(block.nextLine());
			if (row.contains(columnValue)) {
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

	private List<Object> parseRow(String row){
		String[] tempRow = row.split(DEFAULT_SEPARATOR);
		String[] tempSchema = dataSchema.split(DEFAULT_SEPARATOR);
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