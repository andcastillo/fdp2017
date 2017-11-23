package org.db.operator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.db.core.DataBase;
import org.db.core.Node;
import org.db.scan.SeqScan;


/**
 * @author WILMER
 */
public class Projection implements IOperator {

    String type;
    Integer items ;

    public Projection() {
    	
    }

    public void setOperatorName(String name) {
        this.type = name;
    }

    public String getOperatorName() {
        return type;
    }

    public String apply(Node node) {
        if(node.getOperationName().equals("Projection")){ 
            int blockCount = 1;
            String tableName = String.valueOf(node.getTableInput().get(0));
            String TableNameOutput = tableName+"_PR";
            BufferedWriter blockOutput = createOutputFile(TableNameOutput,tableName, blockCount);

            List<String> attr = node.getParameters();
            String schema_txt = openschema(tableName);
            String[] parts_schema = schema_txt.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
            List<String> columns =new ArrayList<String>();

            for (Object object: parts_schema){
                String[] parts_s = String.valueOf(object).split("-");
                columns.add(parts_s[0]);
            }

            List<Integer> pos = new ArrayList<Integer>();
            for (Object object: attr){
            	pos.add(columns.indexOf(String.valueOf(object)));
            	}

            List<String> SchemaProje =new ArrayList<String>();
            for (Integer i: pos){ SchemaProje.add(parts_schema[i]);}

            String lineschema = "";

            for (Object object: SchemaProje){ lineschema +=","+object;}

            try {
                writeschema(TableNameOutput,tableName,lineschema.substring(1));
            } catch (IOException ex) {
                Logger.getLogger(Projection.class.getName()).log(Level.SEVERE, null, ex);
            }

            SeqScan scan = new SeqScan(tableName);
            items =0;
            while (scan.hasNext()) {
                List<Object> rowObject = (List<Object>) scan.next();
                try {
                    String rowStr = "";
                    for (Integer i: pos){
                        rowStr += ','+rowObject.get(i).toString();
                    }
                    //System.out.println("Pt:"+rowStr.substring(1));
                    blockOutput.write(rowStr.substring(1));
                    blockOutput.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                items++;
                if(items % DataBase.LIMIT == 0 && scan.hasNext()){
                    blockCount++;
                    close(blockOutput);
                    blockOutput = createOutputFile(TableNameOutput, tableName, blockCount);
                }
            }
            close(blockOutput);

            // ELIMINACION DE REPETIDOS
            Node node1 = new Node();
            node1.addTableInput(TableNameOutput);
            node1.setOperationName("RemoveRepeated");
            IOperator op1 = new RemoveRepeated();
            String table = op1.apply(node1);
            	// ELIMINACION DE ARCHIVOS TEMP PROJECTION
            	DataBase.getInstance().removeTempFile(TableNameOutput);
            	// FIN ELIMINACION DE ARCHIVOS TEMP PROJECTION
            	
            // FIN ELIMINACION DE REPETIDOS

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
}
