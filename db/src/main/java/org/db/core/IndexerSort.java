package org.db.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.List;
import java.util.Scanner;

import com.google.common.collect.LinkedHashMultimap;

public class IndexerSort {
    
	public static Hashtable<String, Hashtable<String, LinkedHashMultimap<String, String>>> hashIndexesSort = new Hashtable<String, Hashtable<String, LinkedHashMultimap<String, String>>>();
    public static Hashtable<String, Hashtable<String, LinkedHashMultimap<String, String>>> btreeIndexesSort = new Hashtable<String, Hashtable<String, LinkedHashMultimap<String, String>>>();
    
    private static final String DEFAULT_SEPARATOR = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
    private Schema dataSchema; 		//Contiene la informacion del esquema a escanear y ordenar.
    private int countBlocks = 1;	//Contador de bloques.
    private int blockSize = 10;			//Limite del tama�o de cada bloque.
    private Scanner inputStream; 
    
    public IndexerSort () {
    	
    }

    //Indexa ordenadamente la tabla segun la columna por la que se desea ordenar.
    public void indexTableSort(String tableName, String columnOrder) {
        updateSchema(tableName);
        addEntriesForTableInIndexStructures(tableName);
        Attribute[] schemaAttributes = dataSchema.getAttribute();
        for (int i = 0; i < schemaAttributes.length; i++) {
            if(schemaAttributes[i].getColumnName().equals(columnOrder)){
                
            	if(!columnIsIndexed(tableName, columnOrder)){
            		addIndex(tableName, schemaAttributes[i]);
            		
            	}  	
            }
        }
    }
    

   //Añade el index segun la tabla y el atributo de ordenamiento.
    private void addIndex(String tableName, Attribute attribute) {
       
        
        LinkedHashMultimap<String, String> hashMultimap = LinkedHashMultimap.create();
        LinkedHashMultimap<String, String> hashMultimap2 = LinkedHashMultimap.create();
        
        
        countBlocks = 1;
        inputStream = nextBlock(tableName);
        List<List<Object>> lsKeyBlockl =  new ArrayList<List<Object>>();
        
        do {
            int countRows = 0;
            while (countRows < blockSize && inputStream.hasNext()) {
                String row = inputStream.nextLine();
                String[] tempRow = row.split(DEFAULT_SEPARATOR);
                String key = tempRow[attribute.getIndex()];
                String blockLine = (countBlocks - 1) + "," + countRows;
                if (attribute.getScan().equals("hash") || attribute.getType().equals("String")) {
                     List<Object> ls = new ArrayList<Object>();
                     ls.add(key);
                     ls.add(blockLine);
                	 lsKeyBlockl.add(ls);
                } else {
                   
                	List<Object> ls = new ArrayList<Object>();
                    ls.add(key);
                    ls.add(blockLine);
               	 	lsKeyBlockl.add(ls);
                }
                countRows++;
            }
            inputStream = nextBlock(tableName);
        } while (inputStream != null);
        if (attribute.getScan().equals("hash") || attribute.getType().equals("String") ) {
            
        	Collections.sort(lsKeyBlockl,
    				new Comparator<List<Object>>() {

    					@Override
    					public int compare(List<Object> pO1,
    							List<Object> pO2) {
    						return String.valueOf(pO1.get(0)).compareTo(String.valueOf(pO2.get(0)));
    						
    					}

    				});
        	for (List<Object> lst : lsKeyBlockl) {
        		
        		hashMultimap2.put(lst.get(0)+"", lst.get(1)+"");
    			//System.out.println(lst);
    		}
        	

        	
        	
        	hashIndexesSort.get(tableName).put(attribute.getColumnName(), hashMultimap2);
        	
        } else {
            
        	Collections.sort(lsKeyBlockl,
    				new Comparator<List<Object>>() {

        		@Override
                public int compare(List<Object> pO1,
						List<Object> pO2) {

                    if (Integer.valueOf(pO1.get(0)+"") > Integer.valueOf(pO2.get(0)+"")) {
                        return 1;
                    } else if (Integer.valueOf(pO1.get(0)+"") < Integer.valueOf(pO2.get(0)+"")) {
                        return -1;
                    }
                    return 0;
                }

    			});
        	
        	for (List<Object> lst : lsKeyBlockl) {
        		hashMultimap.put(lst.get(0)+"", lst.get(1)+"");
        		
    			//System.out.println(lst);
    		}
        	
        	btreeIndexesSort.get(tableName).put(attribute.getColumnName(), hashMultimap);

        }
        this.close();
    }

    //Actualiza y carga el esquema segun la tabla que se va a indexar.
    private void updateSchema(String tableName){
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

    
    
  //Verifica si la tabla y la columna esta indexada.
    public Boolean columnIsIndexed (String tableName, String columnName) {
        return columnIsHashIndexed(tableName, columnName) || columnIsBTreeIndexed(tableName, columnName);
    }
  //Verifica si la tabla esta indexada  en tree
    public Boolean columnIsBTreeIndexed (String tableName, String columnName) {
        return btreeIndexesSort.containsKey(tableName) && btreeIndexesSort.get(tableName).get(columnName) != null;
    }
  //Verifica si la tabla esta indexada  en hash
    public Boolean columnIsHashIndexed (String tableName, String columnName) {
        return hashIndexesSort.containsKey(tableName) && hashIndexesSort.get(tableName).containsKey(columnName) ;
    }

   //Añade las estructuras de Index
    private void addEntriesForTableInIndexStructures(String tableName) {
        btreeIndexesSort.put(tableName, new Hashtable<String, LinkedHashMultimap<String, String>>());
        hashIndexesSort.put(tableName, new Hashtable<String, LinkedHashMultimap<String, String>>());
    }
    
    private Scanner nextBlock(String tableName){
        File file = new File("data/myDB/"+tableName+"/"+countBlocks+".csv");
        try {
            inputStream = new Scanner(file);
        } catch (FileNotFoundException e) {
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
