package org.db;

import org.db.core.DataBase;
import org.db.core.Node;
import org.db.operator.IOperator;
import org.db.operator.Selection;

public class TestSelection {

    public static void main(String[] args) {


        // PROYECCION
		DataBase.initDataBase("myDB");//Inicializar la base de datos para obtener los esquemas
        Node node = new Node();

        node.addTableInput("B");
        node.setOperationName("Selection");
        node.addParameters("x=\"Iris+\"");
       // node.setWhereCondition("=", "2");

        IOperator op = new Selection();
        String rselt = op.apply(node);
 
        System.out.println("Salida: "+rselt);
    }

}
