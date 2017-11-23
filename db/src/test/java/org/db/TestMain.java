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

		//Primera pregunta
/*        Node node = new Node();
        node.setOperationName("Projection");
        node.addTableInput("cliente");        
        node.addParameters("Ciudad");
        node.addParameters("Activo");
        
        Node node1 = new Node();
        node1.setOperationName("Selection");
        node1.addTableInput(node);        
        node1.addParameters("Activo=true");
  */      
        
/*
        Node nodec = new Node();
        nodec.setOperationName("Join");
        nodec.addTableInput("cliente");
        nodec.addTableInput("factura");        
        nodec.addParameters("client_id");
        nodec.addParameters("client_id");
        
        Node nodep = new Node();
        nodep.setOperationName("Join");
        nodep.addTableInput(nodec);
        nodep.addTableInput("factura");        
        nodep.addParameters("factura_id");
        nodep.addParameters("factura_id");
        //client_id-Integer,Nombre-String,Ciudad-String,Activo-Boolean,clientefactura_JO.factura_id-Integer,clientefactura_JO.cliente_id-Integer,clientefactura_JO.product_id-Integer,clientefactura_JO.Nproducts-Integer,factura.client_id-Integer,factura.Nombre-String,factura.Ciudad-String,factura.Activo-Boolean
        Node nodes = new Node();
        nodes.setOperationName("Projection");
        nodes.addTableInput(nodep);        
        nodes.addParameters("Nombre");//nombre cliente
        nodes.addParameters("client_id");//nombre cliente
        nodes.addParameters("clientefactura_JO.Nproducts");
        nodes.addParameters("clientefactura_JO.factura_id");
        nodes.addParameters("factura.Nombre");//nombre producto*/
  
	      Node nodec = new Node();
	        nodec.setOperationName("Join");
	        nodec.addTableInput("clientefactura_JOfactura_JO_PR_RR_WHERE_RR");
	        nodec.addTableInput("factura");        
	        nodec.addParameters("client_id");
	        nodec.addParameters("client_id");
	 
/*
        Node node1 = new Node();
        node1.setOperationName("Selection");
        node1.addTableInput(nodes);        
        node1.addParameters("client_id=3");
  */      
		Gson gson = new Gson();
		String jsonExample =  gson.toJson(nodec,  Node.class);//Se convierten los nodos a String 
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
