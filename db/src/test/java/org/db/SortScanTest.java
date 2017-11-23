package org.db;

import org.db.scan.SortScan;

public class SortScanTest {
	public static void main(String[] args) {

		// Hash test
		System.out
				.println("------- Ordenar (table: A Columna: b) alfanumerico-------");

		SortScan sortScan = new SortScan("A", "b");
		while (sortScan.hasNext()) {
			System.out.println(sortScan.next());
		}
		;

		// Btree test
		System.out.println("\n------- Ordenar (table: A Columna: id) numerico -------");

		SortScan sortScan2 = new SortScan("A", "id");
		while (sortScan2.hasNext()) {
			System.out.println(sortScan2.next());
		}
		;

	}
}
