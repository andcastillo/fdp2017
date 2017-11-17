package org.db;

import java.util.ArrayList;
import java.util.List;

import org.db.core.Node;
import org.db.operator.IOperator;
import org.db.operator.RemoveRepeated;

public class TestRemoveRepetedAndSeqScan {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		List<String> tablas = new ArrayList<String>();
		tablas.add("A");
		
		Node node = new Node();
		node.setTableInput(tablas);
		node.setType("RemoveRepeated");
		
		IOperator op = new RemoveRepeated();
		String table = op.apply(node);
		
		System.out.println("Tabla Generada: "+table);
	}

}
