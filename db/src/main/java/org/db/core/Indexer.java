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
    private String dataSchema; 		//Contiene la informacion del esquema a escanea
    private int countBlocks = 1;	//Contador de bloques
    private int blockSize = 10;			//Limite del tama�o de cada bloque
    private Scanner inputStream;

    public Indexer () {}

    public void indexTable(String tableName) {
        readSchema(tableName);
        btreeIndexes.put(tableName, new Hashtable<String, BTree<String, ArrayList<String>>>());
        hashIndexes.put(tableName, new Hashtable<String, HashMultimap<String, String>>());
        String[] tempSchema = dataSchema.split(DEFAULT_SEPARATOR);
        for (int i = 0; i < tempSchema.length; i++) {
            String[] schemaCol = tempSchema[i].split("-");
            if(schemaCol.length == 3){
                addIndex(i, tableName, schemaCol[0], schemaCol[2]);
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

    private void addIndex(int columnIndex, String tableName, String columnName, String methodName) {
        HashMultimap<String, String> hashMultimap = HashMultimap.create();
        BTree <String, ArrayList<String>> btree = new BTree<String, ArrayList<String>>();
        countBlocks = 1;
        inputStream = nextBlock(tableName);
        do {
            int countRows = 0;
            while (countRows < blockSize && inputStream.hasNext()) {
                String row = inputStream.nextLine();
                String[] tempRow = row.split(DEFAULT_SEPARATOR);
                String key = tempRow[columnIndex];
                String blockLine = (countBlocks - 1) + "," + countRows;
                if (methodName.equals("hash")) {
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
        if (methodName.equals("hash")) {
            hashIndexes.get(tableName).put(columnName, hashMultimap);
        } else {
            btreeIndexes.get(tableName).put(columnName, btree);
        }
        this.close();
    }

    private void readSchema(String tableName){
        File file = new File("data/myDB/"+tableName+"/schema.txt"); //Se carga el esquema
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
