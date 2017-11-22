package org.db.scan;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.db.core.DataBase;
import org.db.core.IndexerSort;

public class SortScan implements Iterator<List<Object>>{


	private static final String DEFAULT_SEPARATOR = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

	private String tableName; 		//Nombre de la tabla (Directorio) a escanear.
	private String columnNameSort;   //Nombre de la columna a ordenar.
	private String dataSchema; 		//Contiene la informacion del esquema a ordenar.
	private IndexerSort indexerSort;  
	private Iterator<String> blockLinesSort;  
	private Iterator<String> blockLinesSortTree;  
	private String indexMethod;   //Contiene el metodo de index


	
	//Realiza el sortScan a partir de el nombre de la tabla y la columna por la que desea ordenar.
	public SortScan(String tableName, String columnName) {
		this.tableName = tableName;
		this.columnNameSort = columnName;
		//this.indexer = indexer;
		loadSchema();
		int blockCount = 1;
		String blockLine = "";
		IndexerSort indexerSort = new IndexerSort();
	    indexerSort.indexTableSort(tableName, columnName); //Se indexa la tabla y se ordena segun la columna.
	    
	    this.indexerSort= indexerSort;
	    
		//Verifica  cmo se indexo la tabla
		if (indexerSort.columnIsHashIndexed(tableName, columnNameSort)) {
			indexMethod = "hash";
		} else if (indexerSort.columnIsBTreeIndexed(tableName, columnNameSort)) {
			indexMethod = "btree";
		} 
		
		//Crea la tabla intermedia que es la tabla ordenada segun la columna.
		if (indexMethod.equals("hash")) {
			blockLinesSort = IndexerSort.hashIndexesSort.get(tableName).get(columnName).values().iterator();
			
			Iterator<String>  tmpBlockLinesSort = IndexerSort.hashIndexesSort.get(tableName).get(columnName).values().iterator();
			String TableNameOutput = tableName+columnName+"_SS"; //La tabla de salida es un directorio: Nombretabla+nombrecolumna_SS Ej: Ab_SS/A_1.csv
			
			
			BufferedWriter blockOutput = 
					createOutputFile(TableNameOutput, blockCount);	//Se crea el primer bloque en el direcctorio Nombretabla+nombrecolumna_SS Ej: Ab_SS/A_1.csv
			
			int countRows=0;
			while (tmpBlockLinesSort.hasNext()) {
					blockLine =  tmpBlockLinesSort.next();
					List<Object> rowObject =findRowInBlock(blockLine);
					
					try {
						String rowStr = "";							//Creo un String con la primera fila  345,ABC,false,90.3...
						for (Object object: rowObject){
							rowStr = rowStr+","+String.valueOf(object);	
							}
						blockOutput.write(rowStr.substring(1));		//Se escribe en el fichero.
						blockOutput.newLine();
						countRows++;
						} catch (IOException e) {
							e.printStackTrace();
						}
					
					if(countRows > DataBase.LIMIT-1 && tmpBlockLinesSort.hasNext()){	//Cuando se llena el un bloque se crea uno nuevo en el mismo directorio si existe un seguiente registro.
						blockCount++;
						close(blockOutput);												//Se cierra el bloque anterior.
						blockOutput = createOutputFile(TableNameOutput, blockCount);	
						countRows=0;
					}
			}
			
			close(blockOutput);							//Se cierra el ultimo bloque.
			writeschema(tableName, TableNameOutput);
			System.out.println("Se crea la tabla ordena "+TableNameOutput+"\n");
		} else {
			
			blockLinesSortTree = IndexerSort.btreeIndexesSort.get(tableName).get(columnNameSort).values().iterator();
			 Iterator<String>  tmpBlockLinesSort = IndexerSort.btreeIndexesSort.get(tableName).get(columnNameSort).values().iterator();
			 
			 String TableNameOutput = tableName+columnName+"_SS"; //La tabla de salida es un directorio: Nombretabla+nombrecolumna_SS Ej: Ab_SS/A_1.csv
			 BufferedWriter blockOutput = 
						createOutputFile(TableNameOutput, blockCount);	//Se crea el primer bloque en el direcctorio Nombretabla+nombrecolumna_SS Ej: Ab_SS/A_1.csv
			
			int countRows=0;	
			while(tmpBlockLinesSort.hasNext()){
				
				
				blockLine =  tmpBlockLinesSort.next();
					List<Object> rowObject =findRowInBlock(blockLine);
					
					try {
						String rowStr = "";					//Creo un String con la primera fila ej: 123,abc,false,1.2.
						for (Object object: rowObject){
							rowStr = rowStr+","+String.valueOf(object);	
							}
						blockOutput.write(rowStr.substring(1));					//Se escribe en el fichero
						blockOutput.newLine();
						countRows++;
						} catch (IOException e) {
							e.printStackTrace();
						}
					
					if(countRows > DataBase.LIMIT-1 && tmpBlockLinesSort.hasNext()){	//Cuando se llena el un bloque se crea uno nuevo en el mismo directorio si existe un seguiente registro
						blockCount++;
						close(blockOutput);												//Se cierra el bloque anterior
						blockOutput = createOutputFile(TableNameOutput, blockCount);	
						countRows=0;
					}
					
			}
			close(blockOutput);							//Se cierra el ultimo bloque
			writeschema(tableName, TableNameOutput);
			System.out.println("Se crea la tabla ordena "+TableNameOutput+"\n");
		}
		
		loadSchema();
	
	}

	//Se pregunta si hay siguiente en un bloque
	//De lo contrario se carga el siguiente bloque y se inicia nuevamente el contador de registros
	@Override
	public boolean hasNext() {
		if (this.indexMethod.equals("hash")) {
			return this.blockLinesSort.hasNext();
		} else {
			return this.blockLinesSortTree.hasNext();
		}
	}

	@Override
	public List<Object> next() {
		String blockLine = "";
		if (this.indexMethod.equals("hash")) {
			//For hash table
			if (this.blockLinesSort.hasNext()) {
				blockLine = this.blockLinesSort.next();
			}
		} else {
			if (this.blockLinesSortTree.hasNext()) {
				blockLine = this.blockLinesSortTree.next();
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
	
	public BufferedWriter createOutputFile(String dirStr , int block){
		File dir = new File("data/myDB/"+dirStr);
		if(!dir.exists()){
			dir.mkdirs();
		}
        try {
    		File file = new File(dir+"/"+block+".csv");        	        	
			return new BufferedWriter(new FileWriter(file));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return null;
		
	}
	
	public void writeschema(String source, String destination){
        
		try {
			File inFile = new File("data/myDB/"+source+"/schema.txt");
			File outFile = new File("data/myDB/"+destination+"/schema.txt");

			FileInputStream in = new FileInputStream(inFile);
			FileOutputStream out = new FileOutputStream(outFile);
			int c;
			while( (c = in.read() ) != -1)
				out.write(c);

			in.close();
			out.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
    }
	
	public void close(BufferedWriter output){
		if ( output != null ) {
            try {
				output.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
          }
	}
	
}