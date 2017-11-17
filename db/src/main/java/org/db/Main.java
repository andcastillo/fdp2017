package org.db;

import java.util.Scanner;

import org.db.core.DataBase;


public class Main {
	static Scanner scanner;
	
	public static void main(String[] args) {
		System.out.println ("Consola SQL");
		System.out.println ("Por favor introduzca una consulta:");
		String input = "";

		DataBase.initDataBase("myDB");
		//Los siguientes prints en pantalla muestran como se usa el acceso a la bd
		System.out.println("Como acceder a la base de datos:"+DataBase.getInstance().getSchemaMaps().size());
		System.out.println("Como acceder a la base de datos:"+
					DataBase.getInstance().getSchemaMaps().get("A"));//Obtenemos el esquema de A (Imprime Schema@4554617c puesto que es un objeto)
		System.out.println("Como acceder a la base de datos:"+
				DataBase.getInstance().getSchemaMaps().get("A").getAttribute()[0].getColumnName());//Obtenemos el esquema de A y luego los tipos de datos de A
		
		scanner = new Scanner (System.in);
		
		boolean status = true;
		while (status) {
			input = scanner.nextLine ();				//Recibe el texto ingresado por consola
			if(input.equals("0")||input.equals("Exit")){//Dos opciones para salir digitando 0 o Exit
				status = false;
			}else{
				DataBase.getInstance().query(input);			//llamado a ejecutar consulta a la base de datos
			}
			
			System.out.println ("Consulta: \"" + input +"\"");
		}
		System.out.println("Saliendo...");
	} //Cierre del main
	
	
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
