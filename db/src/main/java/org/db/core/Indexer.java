package org.db.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import java.util.Hashtable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.collect.Multimap;

public class Indexer {
    public static Hashtable<String, Hashtable<String, HashMultimap<String, String>>> hashIndexes = new Hashtable<>();
    public static Hashtable<String, Hashtable<String, TreeMultimap<String, String>>> btreeIndexes = new Hashtable<>();
    public static Hashtable<String, Hashtable<String, TreeMultimap<Number, String>>> btreeNumberIndexes = new Hashtable<>();
    private static final String DEFAULT_SEPARATOR = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
    private Schema dataSchema; 		//Contiene la informacion del esquema a escanea
    private int countBlocks = 1;	//Contador de bloques
    private int blockSize = 10;			//Limite del tama�o de cada bloque
    private Scanner inputStream;

    public Indexer () {}

    public void indexTable(String tableName) {
        updateSchema(tableName);
        addEntriesForTableInIndexStructures(tableName);
        Attribute[] schemaAttributes = dataSchema.getAttribute();
        for (int i = 0; i < schemaAttributes.length; i++) {
            if(!schemaAttributes[i].getScan().equals("none")){
                addIndex(tableName, schemaAttributes[i]);
            }
        }
    }

    public Boolean columnIsIndexed (String tableName, String columnName) {
        return columnIsHashIndexed(tableName, columnName) || columnIsBTreeIndexed(tableName, columnName);
    }

    public Boolean columnIsBTreeIndexed (String tableName, String columnName) {
        return (btreeIndexes.containsKey(tableName) && btreeIndexes.get(tableName).containsKey(columnName)) ||
                (btreeNumberIndexes.containsKey(tableName) && btreeNumberIndexes.get(tableName).containsKey(columnName));
    }

    public Boolean columnIsHashIndexed (String tableName, String columnName) {
        return hashIndexes.containsKey(tableName) && hashIndexes.get(tableName).containsKey(columnName);
    }

    private Boolean isTableInTableInIndexStructures(String tableName) {
        return btreeIndexes.containsKey(tableName) || hashIndexes.containsKey(tableName) || btreeNumberIndexes.containsKey(tableName);
    }

    private void addEntriesForTableInIndexStructures(String tableName) {
        btreeIndexes.put(tableName, new Hashtable<>());
        btreeNumberIndexes.put(tableName, new Hashtable<>());
        hashIndexes.put(tableName, new Hashtable<>());
    }

    public String indexColumn(String tableName, String attributeName) {
        updateSchema(tableName);
        if (!isTableInTableInIndexStructures(tableName)) {
            addEntriesForTableInIndexStructures(tableName);
        }
        if (!columnIsIndexed(tableName, attributeName)) {
            Attribute[] schemaAttributes = dataSchema.getAttribute();
            for (int i = 0; i < schemaAttributes.length; i++) {
                if(schemaAttributes[i].getColumnName().equals(attributeName) &&
                        schemaAttributes[i].getScan().equals("none")){
                    String scanMethod = "hash";
                    if (schemaAttributes[i].getType().equals("Integer") ||
                            schemaAttributes[i].getType().equals("Double")) {
                        scanMethod = "btree";
                    }
                    schemaAttributes[i].setScan(scanMethod);
                    addIndex(tableName, schemaAttributes[i]);
                    return  scanMethod;
                }
            }
        }
        return null;
    }

    private void addIndex(String tableName, Attribute attribute) {
        HashMultimap hash = HashMultimap.create();
        TreeMultimap tree = TreeMultimap.create();
        countBlocks = 1;
        inputStream = nextBlock(tableName);
        do {
            int countRows = 0;
            while (countRows < blockSize && inputStream.hasNext()) {
                String row = inputStream.nextLine();
                String[] tempRow = row.split(DEFAULT_SEPARATOR);
                Object key = tempRow[attribute.getIndex()];
                if (attribute.getType().equals("Integer")) {
                    key = Integer.parseInt((String) key);
                } else if (attribute.getType().equals("Double")) {
                    key = Double.parseDouble((String) key);
                }
                String blockLine = (countBlocks - 1) + "," + countRows;
                if (attribute.getScan().equals("hash")) {
                    hash.put(key, blockLine);
                } else {
                    tree.put(key, blockLine);
                }
                countRows++;
            }
            inputStream = nextBlock(tableName);
        } while (inputStream != null);
        if (attribute.getScan().equals("hash")) {
            hashIndexes.get(tableName).put(attribute.getColumnName(), hash);
        } else if (attribute.getType().equals("Integer") || attribute.getType().equals("Double")) {
            btreeNumberIndexes.get(tableName).put(attribute.getColumnName(), tree);
        }
        else {
            btreeIndexes.get(tableName).put(attribute.getColumnName(), tree);
        }
        this.close();
    }

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

    private Scanner nextBlock(String tableName){
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
