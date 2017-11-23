package org.db.operator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.db.core.Attribute;
import org.db.core.DataBase;
import org.db.core.Node;
import org.db.core.Schema;
import org.db.scan.SeqScan;

public class Join implements IOperator{

	String type;
	Set<List<Object>> items ;// Variable que almacena los registros resultantes del join
	private Schema dataSchema1; 	
	private Schema dataSchema2; 	
	
	
		public void setOperatorName(String name) {
		this.type = name;
	}

	public String getOperatorName() {
		return type;
	}

	public String apply(Node node) {		
		if(node.getOperationName().equals("Join")){
			
			
			int blockCount = 1;
			
			int numColumnTbl1=0;
			int numColumnTbl2=0;
			int countRows = 0;
			String tableName1 = String.valueOf(node.getTableInput().get(0));//Se obtiene el nombre de la tabla 1
			String tableName2 = String.valueOf(node.getTableInput().get(1));//Se obtiene el nombre de la tabla 2
			String columnTbl1 = node.getParameters().get(0);//Se obtiene el nombre de la columna 1
			String columnTbl2 = node.getParameters().get(1);//Se obtiene el nombre de la columna 2
			
			
			String TableNameOutput = tableName1+tableName2+"_JO"; //La tabla de salida es un directorio con nombretabla1 + Nombretabla2_JO::::: ej AB_JO
			
			BufferedWriter blockOutput = 
					createOutputFile(TableNameOutput, blockCount);	//Se crea el primer bloque en el direcctorio Ej: AB_JO/AB_1.csv 
			
			
			SeqScan scanTbl1 = new SeqScan(tableName1);					//Se realiza un seq-Scan Tabla1
			

			this.dataSchema1 = DataBase.getInstance().getSchemaMaps().get(tableName1);	//Se carga el schema de la tabla 1
			this.dataSchema2 = DataBase.getInstance().getSchemaMaps().get(tableName2);  //Se carga el schema de la tabla 2
			  
			//Obtener el index de la columna de la tabla 1 por la cual quiero comparar.
			for(int i=0 ;i< dataSchema1.getLength();i++){
				
				Attribute columnAttribute = dataSchema1.getAttribute()[i];
			    if(columnAttribute.getColumnName().equals(columnTbl1)){
			    	numColumnTbl1=columnAttribute.getIndex();
			    	break;
			    }
			}
			
			//Obteer el index de la columna de la tabla 2 por la cual quiero comparar.
			for(int i=0 ;i< dataSchema2.getLength();i++){
				
				Attribute columnAttribute = dataSchema2.getAttribute()[i];
			    if(columnAttribute.getColumnName().equals(columnTbl2)){
			    	numColumnTbl2=columnAttribute.getIndex();
			    	break;
			    }
			}
	

			
			while (scanTbl1.hasNext()) {

				List<Object> rowObjectTbl1 = (List<Object>) scanTbl1.next();//Obtengo la primera linea TABLA 1
				
				String columnValueTbl1 = rowObjectTbl1.get(numColumnTbl1)+"";  //Obtengo el valor de la columna TABLA 1
				
				String rowStrTbl1 = "";	
				for (Object object: rowObjectTbl1){
					rowStrTbl1 = rowStrTbl1+","+String.valueOf(object);	
				}

				SeqScan scanTbl2 = new SeqScan(tableName2);					//Se realiza un seq-Scan Tabla2
				
				
				while (scanTbl2.hasNext()) {
							
					
				List<Object> rowObjectTbl2 = (List<Object>) scanTbl2.next(); //Obtengo la primera linea TABLA 2

				String columnValueTbl2 = rowObjectTbl2.get(numColumnTbl2)+"";  //Obtengo el valor de la columna TABLA 2
				
				if(columnValueTbl1.equals(columnValueTbl2)){
				
							try {
								String rowStrTbl2 = ""; // Creo un String con la  primera fila ej:  1,abc,false,4.4
								for (Object object : rowObjectTbl2) {
									rowStrTbl2 = rowStrTbl2 + ","
											+ String.valueOf(object);
								}
	                            String rowsJoined = rowStrTbl1.substring(1)+rowStrTbl2;
								blockOutput.write(rowsJoined); // Se escribe  en el fichero
								blockOutput.newLine();
							
								countRows++;
							
								
								
							} catch (IOException e) {
								e.printStackTrace();
							}
							
							
							if(countRows > DataBase.LIMIT-1 && (scanTbl1.hasNext() || scanTbl2.hasNext())){	//Cuando se llena el un bloque se crea uno nuevo en el mismo directorio si existe un seguiente registro
								blockCount++;
								close(blockOutput);							//Se cierra el bloque anterior
								blockOutput = createOutputFile(TableNameOutput, blockCount);	
								countRows=0;
							}
					}			
				}
			}
		

			close(blockOutput);							//Se cierra el ultimo bloque
			writeschemaJoin(tableName1, tableName2,TableNameOutput);
			node.setTableNameOutput(TableNameOutput);
			
			
			return TableNameOutput;
		}
		return null;
	}
	
	public BufferedWriter createOutputFile(String dirStr, int block){
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
	
//Escribo el schema.txt de la tabla resultante del JOIN.
	public void writeschemaJoin(String source1, String source2, String destination){
          
		try {
			
			Attribute[]  atributeTbl1 = this.dataSchema1.getAttribute();
			Attribute[]  atributeTbl2 = this.dataSchema2.getAttribute();
			
			String dataLineSchema = "";
		
			List<Integer> lsRepeatedCol2 = new ArrayList<Integer>();
			Boolean repeat= false;
			
			for (int i = 0; i < atributeTbl1.length; i++) {
				
				for (int j = 0; j < atributeTbl2.length; j++) {
				
					if(atributeTbl1[i].getColumnName().equals(atributeTbl2[j].getColumnName())){
						lsRepeatedCol2.add(atributeTbl2[j].getIndex());
						repeat=true;
						break;
					}
			
				}
				dataLineSchema+= repeat ? source1+"."+atributeTbl1[i].getColumnName()+"-"+atributeTbl1[i].getType()+"-"+atributeTbl1[i].getScan()+"," : atributeTbl1[i].getColumnName()+"-"+atributeTbl1[i].getType()+"-"+atributeTbl1[i].getScan()+",";
                    repeat=false;
			}
			
			for (int i = 0; i < atributeTbl2.length; i++) {
				
				if(lsRepeatedCol2.contains(i)){
					dataLineSchema+= source2+"."+atributeTbl1[i].getColumnName()+"-"+atributeTbl1[i].getType()+"-"+atributeTbl1[i].getScan()+"," ;
				}else{
					dataLineSchema+= atributeTbl2[i].getColumnName()+"-"+atributeTbl2[i].getType()+"-"+atributeTbl2[i].getScan()+",";
				}
			
			}
			
			dataLineSchema = dataLineSchema.replace("-null", "");
			dataLineSchema =dataLineSchema.substring(0,dataLineSchema.length()-1);
		    
			System.out.println(dataLineSchema);
			
			File outFile = new File("data/myDB/"+destination+"/schema.txt");
			BufferedWriter bw;
		        
		        bw = new BufferedWriter(new FileWriter(outFile));
		        bw.write(dataLineSchema);
		        bw.close();
			
		} catch(IOException e) {
			e.printStackTrace();
		}
    }
}
