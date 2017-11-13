import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

public class IndexScan implements Iterator<List<Object>>{


    private static final String DEFAULT_SEPARATOR = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

	private String tableName; 		//Nombre de la tabla (Directorio) a escanear
	private int limit = 10;			//Limite del tama�o de cada bloque
	private int countBlocks = 1;	//Contador de bloques
	private int countRows = 1;		//Contador de registros dentro de cada bloque
	private String dataSchema; 		//Contiene la informacion del esquema a escanear
	private Scanner inputStream;


	public IndexScan(String tableName) {
	}

	//Se pregunta si hay siguiente en un bloque
	//De lo contrario se carga el siguiente bloque y se inicia nuevamente el contador de registros
	@Override
	public boolean hasNext() {
	}

	@Override
	public List<Object> next() {
	}
	
	public void close(){

	}
	
	public List<Object> parseRow(String row , String schema){
		String[] tempRow = row.split(DEFAULT_SEPARATOR);
		String[] tempSchema = schema.split(DEFAULT_SEPARATOR);
		List<Object> list = null;
		if(tempRow.length == tempSchema.length){
			list = new ArrayList<>();
			for (int i = 0; i < tempRow.length; i++) {
				String[] schemaCol = tempSchema[i].split("-");
				String data = tempRow[i];
				if(schemaCol[1].equals("Integer")){
					list.add(Integer.parseInt(data));
				}else if (schemaCol[1].equals("Double")){
					list.add(Double.parseDouble(data));
				}else if (schemaCol[1].equals("Boolean")){
					list.add(Boolean.parseBoolean(data));
				}else {//String
					list.add(String.valueOf(data));
				}			
			}
		}
		countRows++;
		return list;
	}
}

