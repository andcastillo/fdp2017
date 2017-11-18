package org.db.operator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.db.core.Node;
import org.db.scan.SeqScan;

public class RemoveRepeated implements IOperator{

	String type;
	Set<List<Object>> items ;// Variable que almacena los registros unicos 

		public void setOperatorName(String name) {
		this.type = name;
	}

	public String getOperatorName() {
		return type;
	}

	public String apply(Node node) {		
		if(node.getOperationName().equals("RemoveRepeated")){
			int limit = 10;
			int blockCount = 1;
			String tableName = String.valueOf(node.getTableInput().get(0));
			String TableNameOutput = tableName+"_RR";					//La tabla de salida es un directorio con nombre Ej: A_RR
			BufferedWriter blockOutput = 
					createOutputFile(TableNameOutput,tableName, blockCount);	//Se crea el primer bloque en el direcctorio Ej: A_RR/A_1.csv 
			SeqScan scan = new SeqScan(tableName);					//Se realiza un seq-Scan
			items = new HashSet<List<Object>>();
			while (scan.hasNext()) {
				List<Object> rowObject = (List<Object>) scan.next();
				if(items.add(rowObject)){							//Identifico los repetido si se a�ade correctamente true (no hay duplicado), false (hay duplicado)
					try {
						String rowStr = "";							//Creo un String con la primera fila ej: 123;cool;...
						for (Object object: rowObject){
							rowStr = rowStr+","+String.valueOf(object);	
							}
						//System.out.println("Pt:"+ rowStr.substring(1));
						blockOutput.write(rowStr.substring(1));					//Se escribe en el fichero
						blockOutput.newLine();
						} catch (IOException e) {
							e.printStackTrace();
						}							
					if(items.size()% limit == 0 && scan.hasNext()){	//Cuando se llena el un bloque se crea uno nuevo en el mismo directorio si existe un seguiente registro
						blockCount++;
						close(blockOutput);							//Se cierra el bloque anterior
						blockOutput = createOutputFile(TableNameOutput, tableName, blockCount);						
					}
				}
			}
			close(blockOutput);							//Se cierra el ultimo bloque
			writeschema(tableName, TableNameOutput);
			node.setTableNameOutput(TableNameOutput);
			return TableNameOutput;
		}
		return null;
	}
	
	public BufferedWriter createOutputFile(String dirStr, String tableName, int block){
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
	
	

}
