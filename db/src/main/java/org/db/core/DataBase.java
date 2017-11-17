package org.db.core;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import org.db.operator.IOperator;
import org.db.operator.Projection;


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
		List<String> tables = new ArrayList<String>();
		tables.add("A");
		List<String> parameters = new ArrayList<String>();
		parameters.add("id");
		parameters.add("x");
		Node node = new Node("Projection", tables, parameters, "A_ggg", null);
		return node;
	}

	private String executeSQL(Object executionTree) {

		if(executionTree instanceof Node) {
			Node tmp = (Node) executionTree;

			try {
				Class<?> clazz = Class.forName("org.db.operator." + tmp.getOperationName());
				Constructor<?> ctor = clazz.getConstructor();
				Object object;
				try {
					object = ctor.newInstance(new Object[] { });
					if(object instanceof IOperator) {
						return ((IOperator)object).apply(tmp);
					}
				} catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
					// TODO Auto-generated catch block. Never ever do this. I'm lazy
					e.printStackTrace();
					return null;
				} 
				
			} catch (NoSuchMethodException | SecurityException | ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
			
		}
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
		Scanner inputStream;
		Schema schema = null;
		try {
			inputStream = new Scanner(dirTable);

			while (inputStream.hasNext()) {
				//Las lineas vienen en formato: nombre-tipo-index, nombre-tipo-index, ...
				String dataSchema = inputStream.nextLine(); 
				String[] row = dataSchema.split(DEFAULT_SEPARATOR);
				schema = new Schema(table, row.length);
				for (int i = 0; i < row.length; i++) {
					String[] column = row[i].split("-");
					//column[0];//nombre columna		        			
					//column[1];//tipo
					//column[2];//index
					//Adicionar el tipo a la lista en pareja ej: (Apellido,String)..

					String pathIndex = null;//Almacena el path del index creado
					if(column.length > 2){//Si tiene indexacion
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
					}

					Attribute attrib = new Attribute(i, pathIndex, column[0]);
					//Adicionar el Index a la lista en pareja ej: (Apellido,IndexHash)..
					schema.addAttribute(attrib, i);		
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
