package org.db;

import java.util.Scanner;

import org.db.core.DataBase;
import org.db.core.Node;

import com.google.gson.Gson;

public class MainPregunta3 {
	static Scanner scanner;
	
	public static void main(String[] args) {
		System.out.println ("Consola SQL");
		System.out.println ("Por favor introduzca una consulta:");
		String input = "";
 
        Node nodec = new Node();
        nodec.setOperationName("Join");
        nodec.addTableInput("cliente");
        nodec.addTableInput("factura");        
        nodec.addParameters("client_id");
        nodec.addParameters("cliente_id");
        
        Node nodep = new Node();
        nodep.setOperationName("Join");
        nodep.addTableInput(nodec);
        nodep.addTableInput("producto");        
        nodep.addParameters("product_id");
        nodep.addParameters("product_id");
        
        Node nodes = new Node();
        nodes.setOperationName("Projection");
        nodes.addTableInput(nodep);        
        nodes.addParameters("client_id");
        nodes.addParameters("clientefactura_JO.Nombre");//nombre cliente
        nodes.addParameters("Nproducts");
        nodes.addParameters("factura_id");
        nodes.addParameters("producto.Nombre");//nombre producto*/
        nodes.addParameters("Color");
  
        Node nodew1 = new Node();
        nodew1.setOperationName("Selection");
        nodew1.addTableInput(nodes);      
        nodew1.addParameters("Nproducts>46");
        
        Node nodew2 = new Node();
        nodew2.setOperationName("Selection");
        nodew2.addTableInput(nodew1);      
        nodew2.addParameters("Color=azul");
        
	 
		Gson gson = new Gson();
		String jsonExample =  gson.toJson(nodew2,  Node.class);//Se convierten los nodos a String 
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

}
