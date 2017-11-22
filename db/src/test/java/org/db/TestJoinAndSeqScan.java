package org.db;

import org.db.core.DataBase;
import org.db.core.Node;
import org.db.operator.IOperator;
import org.db.operator.Join;
import org.db.operator.RemoveRepeated;

public class TestJoinAndSeqScan {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DataBase.initDataBase("myDB");//Inicializar la base de datos para obtener los esquemas
		Node node = new Node();
		node.addTableInput("A");  //Ingresar tabla 1.
		node.addTableInput("B");  //Ingresar tabla 1.
		node.addParameters("id"); //Ingresar columna tabla 1.
		node.addParameters("id"); //Ingresar columna tabla 2.
		node.setOperationName("Join"); //Nodo operacion.
		
		IOperator op = new Join();
		String table = op.apply(node);
		
		System.out.println("Tabla Generada: "+table);
	}

}
