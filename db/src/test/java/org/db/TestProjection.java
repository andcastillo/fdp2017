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
        List<String> tablas = new ArrayList<String>();
        tablas.add("A");

        List<String> attr = new ArrayList<String>();
        attr.add("id"); // Definir el nombre de atributo
        attr.add("c"); // Definir el nombre de atributo

        Node node = new Node();

        node.setTableInput(tablas);
        node.setParameters(attr);
        node.setType("Projection");

        IOperator op = new Projection();
        String rselt = op.apply(node);

        // ELIMINACION DE REPETIDOS
        List<String> tablasalida = new ArrayList<String>();
        tablasalida.add(rselt);

        Node node1 = new Node();
        node1.setTableInput(tablasalida);

        node1.setType("RemoveRepeated");

        IOperator op1 = new RemoveRepeated();
        String table = op1.apply(node1);

        System.out.println("Salida: "+rselt);
    }

}
