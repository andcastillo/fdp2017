package org.db.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;


public class DataBase {

	private static DataBase instance = null;
	private static final String DEFAULT_SEPARATOR = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";
	private HashMap<String, Schema> schemaMaps;
	private String database;
	static final int LIMIT = 10;			//Limite del tama�o de cada bloque

	//adicionar variables con los esquema y indices

	public DataBase(String database) {
		super();
		this.database = database;
		this.schemaMaps = new HashMap<String, Schema>();
		loadSchema(database);
	}
	
	public static DataBase initDataBase(String database){
		instance = new DataBase(database);
		return instance;
	}	

	public static DataBase getInstance() {
	      return instance;
	}


	public String query(String queryStr){
		Object executionTree = parserSQL(queryStr);//llamado al parseador
		return executeSQL(executionTree); //llamado a ejecutar la consula con el arbol generado por el parseador

	}

	private Object parserSQL(String sql){
		//implementaci�n del parser
		return null;
	}

	private String executeSQL(Object executionTree){
		//implementar un motor de ejecuci�n de consultas
		return null;
	}

	

	public HashMap<String, Schema> getSchemaMaps() {
		return schemaMaps;
	}
	
	public void setSchemaMaps(HashMap<String, Schema> schemaMaps) {
		this.schemaMaps = schemaMaps;
	}
	
	private void loadSchema(String database) {
		System.out.println(System.getProperty("user.dir"));
		File folder = new File("data/"+database);        
		File[] listOfFiles = folder.listFiles(); //Obtiene la lista de archivos
		for (int i = 0; i < listOfFiles.length; i++) {

			if (listOfFiles[i].isDirectory()) 
			{
				//llamado a la funcion auxiliar para cargar esquemas
				auxLoadSchema(database,listOfFiles[i].getName());
			}
		}
	}

	public void auxLoadSchema(String dir, String table){		        
		File dirTable = new File("data/"+dir+"/"+table+"/schema.txt"); 
		Schema schema = new Schema(table);
		Scanner inputStream;
		try {
			inputStream = new Scanner(dirTable);

			while (inputStream.hasNext()) {
				//Las lineas vienen en formato: nombre-tipo-index, nombre-tipo-index, ...
				String dataSchema = inputStream.nextLine(); 
				String[] temp = dataSchema.split(DEFAULT_SEPARATOR);
				for (String columns : temp) {
					String[] column = columns.split("-");
					//column[0];//nombre columna		        			
					//column[1];//tipo
					//column[2];//index

					//Adicionar el tipo a la lista en pareja ej: (Apellido,String)..
					schema.getTypes().put(column[0], column[1]);	

					if(column.length > 2){//Si tiene indexacion
						String pathIndex = "";//Almacena el path del index creado
						switch ( column[2] ) {
						case "btree":
							//Implementar creacion de index btree
							//pathIndex = new IndexarBtree(table)
							//System.out.println("Indexando btree...");		        			           
							break;
						case "hash":
							//Implementar creacion de index hash
							//pathIndex = new IndexarHash(table)
							//System.out.println("Indexando hash...");
							break;
						default:
							System.out.println("Sin index" );
							break;
						}
						//Adicionar el Index a la lista en pareja ej: (Apellido,IndexHash)..
						schema.getIndexes().put(column[0], pathIndex);		        				
					}	  
				}		                   
			}
			inputStream.close();

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Se adiciona el esquema cargado a la lista de esquemas.
		schemaMaps.put(table, schema);
	}


}
