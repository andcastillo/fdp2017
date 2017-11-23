package org.db;

import java.util.Scanner;

import org.db.core.DataBase;
import org.db.core.Node;

import com.google.gson.Gson;

public class MainPregunta1 {
	static Scanner scanner;
	
	public static void main(String[] args) {
		System.out.println ("Consola SQL");
		System.out.println ("Por favor introduzca una consulta:");
		String input = "";

		//Primera pregunta
        Node node = new Node();
        node.setOperationName("Projection");
        node.addTableInput("cliente");        
        node.addParameters("Ciudad");
        node.addParameters("Activo");
        
        Node node1 = new Node();
        node1.setOperationName("Selection");
        node1.addTableInput(node);        
        node1.addParameters("Activo=true");
              
		Gson gson = new Gson();
		String jsonExample =  gson.toJson(node1,  Node.class);//Se convierten los nodos a String 
		System.out.println("Json:"+jsonExample);
		DataBase.initDataBase("myDB");
	    
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
