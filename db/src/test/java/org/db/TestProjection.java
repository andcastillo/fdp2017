package org.db;

import java.util.ArrayList;
import java.util.List;

import org.db.core.Node;
import org.db.operator.IOperator;
import org.db.operator.Projection;
import org.db.operator.RemoveRepeated;

public class TestProjection {

    public static void main(String[] args) {


        // PROYECCION
        Node node = new Node();

        node.addTableInput("A");
        node.addParameters("id");
        node.addParameters("c");
        node.setOperationName("Projection");

        IOperator op = new Projection();
        String rselt = op.apply(node);

        // ELIMINACION DE REPETIDOS

        Node node1 = new Node();
        node1.addTableInput(rselt);

        node1.setOperationName("RemoveRepeated");

        IOperator op1 = new RemoveRepeated();
        String table = op1.apply(node1);

        System.out.println("Salida: "+rselt);
    }

}
