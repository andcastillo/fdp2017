package org.db.operator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.db.core.DataBase;
import org.db.core.Node;
import org.db.scan.SeqScan;


/*
 * @author Eduardo Arango 
 */
public class Selection implements IOperator {

    String type;
    Integer items ;

    public Selection() {
    	
    }

    public void setOperatorName(String name) {
        this.type = name;
    }

    public String getOperatorName() {
        return type;
    }


    public String apply(Node node) {

        return null;
    }

    public static void writeschema(String dirStr, String tableName, String line) throws FileNotFoundException, IOException {
        File dir = new File("data/myDB/"+dirStr);
        if(!dir.exists()){
            dir.mkdirs();
        }
        File fout = new File(dir+"/schema.txt");
        FileOutputStream fos = new FileOutputStream(fout);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        bw.write(line);
        bw.newLine();
        bw.close();
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
            // TODO: Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public void close(BufferedWriter output){
        if ( output != null ) {
            try {
                output.close();
            } catch (IOException e) {
                // TODO: Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public String openschema(String tableName){
        String dataSchema = null;
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
        return dataSchema;
    }

    private static void delete(File file) throws IOException {

        for (File childFile : file.listFiles()) {

            if (childFile.isDirectory()) {
                delete(childFile);
            } else {
                if (!childFile.delete()) {
                    throw new IOException();
                }
            }
        }

        if (!file.delete()) {
            throw new IOException();
        }
    }
}

//EOF!
