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
    Integer items;

    public Selection() {

    }

    public void setOperatorName(String name) {
        this.type = name;
    }

    public String getOperatorName() {
        return type;
    }


    public String apply(Node node) {
        if (node.getOperationName().equals("Selection")) {
            String operation = "none";
            String[] selectionParameters = null;
            if(((String)node.getParameters().get(0)).contains("=")) {
                selectionParameters = ((String)node.getParameters().get(0)).split("=");
                operation = "=";
            }
            else if(((String)node.getParameters().get(0)).contains(">")) {
                selectionParameters = ((String)node.getParameters().get(0)).split(">");
                operation = "=";
            }
            else if(((String)node.getParameters().get(0)).contains("<")) {
                selectionParameters = ((String)node.getParameters().get(0)).split("<");
                operation = "=";
            }


            int limit = 10;
            int blockCount = 1;
            String tableName = String.valueOf(node.getTableInput().get(0));
            String TableNameOutput = tableName + "_WHERE";
            BufferedWriter blockOutput = createOutputFile(TableNameOutput, tableName, blockCount);

            String attr = selectionParameters[0];
            String schema_txt = openschema(tableName);
            String[] parts_schema = schema_txt.split(",");
            List<String> columns = new ArrayList<String>();

            for (Object object : parts_schema) {
                String[] parts_s = String.valueOf(object).split("-");
                columns.add(parts_s[0]);
            }

            List<Integer> pos = new ArrayList<Integer>();

           pos.add(columns.indexOf(attr));


            List<String> SchemaSelect = new ArrayList<String>();
            for (Integer i : pos) {
                SchemaSelect.add(parts_schema[i]);
            }

            String lineschema = "";

            for (Object object : SchemaSelect) {
                lineschema += "," + object;
            }

            try {
                writeschema(TableNameOutput, tableName, schema_txt);
            } catch (IOException ex) {
                Logger.getLogger(Projection.class.getName()).log(Level.SEVERE, null, ex);
            }

            // Funcion de seleccion
            // (WHERE TABLE.COLUMN {CONDITION} CONSTANT)
            SeqScan scan = new SeqScan(tableName);
            items = 0;
            while (scan.hasNext()) {

                List<Object> rowObject = (List<Object>) scan.next();
                try {

                    String rowStr = "";
                    String constant = selectionParameters[1];
                    if (operation.equals("=")) {
                        for (Integer i : pos) {
                            if (constant.equals(rowObject.get(i).toString())) {
                                for (Object j: rowObject){
                                    rowStr += "," +j.toString();
                                }
                            }
                        }
                        if(rowStr.length() > 1) {
                            blockOutput.write(rowStr.substring(1));
                            blockOutput.newLine();
                        }


                    }

                    // if operation; == "<"
                    if (operation.equals("<")) {
                        for (Integer i : pos) {
                            if (Integer.parseInt(constant) < (Integer) rowObject.get(i)) {
                                for (Object j: rowObject){
                                    rowStr += "," +j.toString();
                                }
                            }
                        }

                        if(rowStr.length() > 1) {
                            blockOutput.write(rowStr.substring(1));
                            blockOutput.newLine();
                        }

                    }

                    // if operation; == ">"
                    if (operation.equals(">")) {
                        for (Integer i : pos) {
                            if (Integer.parseInt(constant) > (Integer) rowObject.get(i)) {
                                for (Object j: rowObject){
                                    rowStr += "," +j.toString();
                                }
                            }
                        }

                        if(rowStr.length() > 1) {
                            blockOutput.write(rowStr.substring(1));
                            blockOutput.newLine();
                        }
                    }
                } catch(IOException e){
                    e.printStackTrace();
                }
                // END SELECTION func

                items++;
                if (items % DataBase.LIMIT == 0 && scan.hasNext()) {
                    blockCount++;
                    close(blockOutput);
                    blockOutput = createOutputFile(TableNameOutput, tableName, blockCount);
                }
            }close(blockOutput);
            // ELIMINACION DE REPETIDOS
           Node node1 = new Node();
            node1.addTableInput(TableNameOutput);

            node1.setOperationName("RemoveRepeated");

            IOperator op1 = new RemoveRepeated();
            String table = op1.apply(node1);

            // FIN ELIMINACION DE REPETIDOS

            // ELIMINACION DE ARCHIVOS TEMP PROJECTION
            File file = new File("data/myDB/" + TableNameOutput);
            try {
                delete(file);
            } catch (IOException ex) {
                Logger.getLogger(Projection.class.getName()).log(Level.SEVERE, null, ex);
            }
            // FIN ELIMINACION DE ARCHIVOS TEMP PROJECTION

            node.setTableNameOutput(table);
            return table;
        }
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
                   // throw new IOException();
                }
            }
        }

        if (!file.delete()) {
            //throw new IOException();
        }
    }
}