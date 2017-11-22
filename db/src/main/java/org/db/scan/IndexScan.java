package org.db.scan;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

import org.db.core.Attribute;
import org.db.core.Indexer;
import org.db.core.Schema;

public class IndexScan implements Iterator<List<Object>>{


	private static final String DEFAULT_SEPARATOR = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

	private String tableName; 		//Nombre de la tabla (Directorio) a escanear
	private String columnName;
	private Schema dataSchema; 		//Contiene la informacion del esquema a escanear
	private Indexer indexer;
	private Iterator<String> blockLines;
	private String indexMethod;

	public IndexScan(String tableName, String columnName, Indexer indexer) {
		this.tableName = tableName;
		this.columnName = columnName;
		this.indexer = indexer;
		loadSchema();
		Attribute attribute = new Attribute();
		for (int i = 0; i < this.dataSchema.getAttribute().length; i++) {
			if (this.dataSchema.getAttribute()[i].getColumnName().equals(columnName)) {
				attribute = this.dataSchema.getAttribute()[i];
				break;
			}
		}
		if (indexer.columnIsHashIndexed(tableName, columnName)) {
			indexMethod = "hash";
		} else if (indexer.columnIsBTreeIndexed(tableName, columnName)) {
			indexMethod = "btree";
		} else {
			indexMethod = indexer.indexColumn(tableName, columnName);
		}
		if (indexMethod.equals("hash")) {
			blockLines = Indexer.hashIndexes.get(tableName).get(columnName).values().iterator();
		} else if (attribute.getType().equals("Integer") || attribute.getType().equals("Double")) {
			blockLines = Indexer.btreeNumberIndexes.get(tableName).get(columnName).values().iterator();
		} else {
			blockLines = Indexer.btreeIndexes.get(tableName).get(columnName).values().iterator();
		}
	}

	//Se pregunta si hay siguiente en un bloque
	//De lo contrario se carga el siguiente bloque y se inicia nuevamente el contador de registros
	@Override
	public boolean hasNext() {
		return this.blockLines.hasNext();
	}

	@Override
	public List<Object> next() {
		String blockLine = "";
		if (this.blockLines.hasNext()) {
			blockLine = this.blockLines.next();
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
		String path = "data/myDB/"+tableName+"/schema.txt";
		File file = new File(path); //Se carga el esquema
		try {
			Scanner inputStream = new Scanner(file);
			while (inputStream.hasNext()) {
				this.dataSchema = getSchemaFromString(inputStream.nextLine(), path);
			}
			inputStream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private Schema getSchemaFromString (String schemaString, String path){
		String[] tempSchema = schemaString.split(DEFAULT_SEPARATOR);
		Schema schema = new Schema(path, tempSchema.length);
		for (int i = 0; i < tempSchema.length; i++) {
			String[] schemaCol = tempSchema[i].split("-");
			String scanMethod = "none";
			if(schemaCol.length == 3){
				scanMethod = schemaCol[2];
			}
			schema.addAttribute(new Attribute(i, scanMethod, schemaCol[0], schemaCol[1]), i);
		}
		return schema;
	}

	private List<Object> parseRow(String row){
		String[] tempRow = row.split(DEFAULT_SEPARATOR);
		List<Object> list = null;
		if(tempRow.length == dataSchema.getAttribute().length){
			list = new ArrayList<Object>();
			for (int i = 0; i < tempRow.length; i++) {
				Attribute attribute = dataSchema.getAttribute()[i];
				String data = tempRow[i];
				if(attribute.getType().equals("Integer")){
					list.add(Integer.parseInt(data));
				}else if (attribute.getType().equals("Double")){
					list.add(Double.parseDouble(data));
				}else if (attribute.getType().equals("Boolean")){
					list.add(Boolean.parseBoolean(data));
				}else {//String
					list.add(String.valueOf(data));
				}
			}
		}
		return list;
	}
}