package org.db.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;

import org.db.util.BTree;

import java.util.Hashtable;
import com.google.common.collect.HashMultimap;

public class Indexer {
    public static Hashtable<String, Hashtable<String, HashMultimap<String, String>>> hashIndexes = new Hashtable<String, Hashtable<String, HashMultimap<String, String>>>();
    public static Hashtable<String, Hashtable<String, BTree<String, ArrayList<String>>>> btreeIndexes = new Hashtable<String, Hashtable<String, BTree<String, ArrayList<String>>>>();
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
        return btreeIndexes.containsKey(tableName) && btreeIndexes.get(tableName).get(columnName) != null;
    }

    public Boolean columnIsHashIndexed (String tableName, String columnName) {
        return hashIndexes.containsKey(tableName) && hashIndexes.get(tableName).containsKey(columnName);
    }

    private Boolean isTableInTableInIndexStructures(String tableName) {
        return btreeIndexes.containsKey(tableName) || hashIndexes.containsKey(tableName);
    }

    private void addEntriesForTableInIndexStructures(String tableName) {
        btreeIndexes.put(tableName, new Hashtable<String, BTree<String, ArrayList<String>>>());
        hashIndexes.put(tableName, new Hashtable<String, HashMultimap<String, String>>());
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
        HashMultimap<String, String> hashMultimap = HashMultimap.create();
        BTree <String, ArrayList<String>> btree = new BTree<String, ArrayList<String>>();
        countBlocks = 1;
        inputStream = nextBlock(tableName);
        do {
            int countRows = 0;
            while (countRows < blockSize && inputStream.hasNext()) {
                String row = inputStream.nextLine();
                String[] tempRow = row.split(DEFAULT_SEPARATOR);
                String key = tempRow[attribute.getIndex()];
                String blockLine = (countBlocks - 1) + "," + countRows;
                if (attribute.getScan().equals("hash")) {
                    hashMultimap.put(key, blockLine);
                } else {
                    if (btree.get(key) == null) {
                        ArrayList<String> directions = new ArrayList<String>();
                        btree.put(key, directions);

                    }
                    btree.get(key).add(blockLine);
                }
                countRows++;
            }
            inputStream = nextBlock(tableName);
        } while (inputStream != null);
        if (attribute.getScan().equals("hash")) {
            hashIndexes.get(tableName).put(attribute.getColumnName(), hashMultimap);
        } else {
            btreeIndexes.get(tableName).put(attribute.getColumnName(), btree);
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
