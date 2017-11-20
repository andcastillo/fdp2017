package org.db;

import org.db.core.DataBase;
import org.db.core.Node;
import org.db.operator.IOperator;
import org.db.operator.Projection;

public class TestProjection {

    public static void main(String[] args) {


        // PROYECCION
		DataBase.initDataBase("myDB");//Inicializar la base de datos para obtener los esquemas
        Node node = new Node();

        node.addTableInput("A");
        node.addParameters("id");
        node.addParameters("c");
        node.setOperationName("Projection");

        IOperator op = new Projection();
        String rselt = op.apply(node);
 
        System.out.println("Salida: "+rselt);
    }

}
