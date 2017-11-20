package org.db;

import java.io.IOException;
import java.util.Scanner;

import org.db.core.DataBase;
import org.db.core.Node;

import com.google.gson.Gson;

public class TestMain {
	static Scanner scanner;
	
	public static void main(String[] args) {
		System.out.println ("Consola SQL");
		System.out.println ("Por favor introduzca una consulta:");
		String input = "";


        Node node = new Node();
        node.setOperationName("Projection");
        node.addTableInput("A");
        node.addParameters("c");
        node.addParameters("id");
        

        Node node1 = new Node();
        node1.setOperationName("Projection");
        node1.addTableInput("A");
        node1.addParameters("id");
        node1.addParameters("c");
        node1.addParameters("b");
        node1.addParameters("x");
        
        node.addTableInput(node1);
        
		Gson gson = new Gson();
		String jsonExample =  gson.toJson(node,  Node.class);//Se convierten los nodos a String 
		System.out.println("Json:"+jsonExample);
		DataBase.initDataBase("myDB");
		//Los siguientes prints en pantalla muestran como se usa el acceso a la bd
		//System.out.println("Como acceder a la base de datos:"+DataBase.getInstance().getSchemaMaps().size());
		//System.out.println("Como acceder a la base de datos:"+
		//			DataBase.getInstance().getSchemaMaps().get("A"));//Obtenemos el esquema de A (Imprime Schema@4554617c puesto que es un objeto)
		//System.out.println("Como acceder a la base de datos:"+
		//		DataBase.getInstance().getSchemaMaps().get("A").getAttribute()[0].getColumnName());//Obtenemos el esquema de A y luego los tipos de datos de A

	    
		scanner = new Scanner (System.in);
		
		boolean status = true;
		while (status) {
			input = scanner.nextLine ();				//Recibe el texto ingresado por consola
			String output = "";
			if(input.equals("0")||input.equals("Exit")){//Dos opciones para salir digitando 0 o Exit
				status = false;
			}else{
				input = jsonExample;	//Test Se usa el String para probar las entradas en formato Json
				output = DataBase.getInstance().query(input);			//llamado a ejecutar consulta a la base de datos
			}
			
			System.out.println ("ResultTable:" + output +"");
			//openResult(output);abrir el resultado pero
		}
		System.out.println("Saliendo...");
	} //Cierre del main
	
	/*public static void openResult(String tabla){
		try {
            Runtime obj = Runtime.getRuntime(); 
            obj.exec("notepad "+System.getProperty("user.dir")+"\\data/myDB/"+tabla+"/1.csv");
        } catch (IOException ex) {
             System.out.println("IOException "+ex.getMessage());
        }
	}*/
/*
import java.util.Enumeration;

public class Main {
    public static void main(String[] args){
        // Hash table test
        Indexer aIndexer = new Indexer("A");
        aIndexer.indexTable();
        IndexScan aIndexScan = new IndexScan(aIndexer);
        while (aIndexScan.hasNext()) {
            System.out.println(aIndexScan.next());
        };
        // Btree test
        Indexer bIndexer = new Indexer("B");
        aIndexer.indexTable();
    }
*/
}
