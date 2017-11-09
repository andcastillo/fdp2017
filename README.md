# fdp2017
## Evaluación práctica de los conceptos fundamentales de las bases de datos

## Metodología:
Este es un trabajo grupal para la asignatura Fundamentos de bases de datos, 2 semestre 2017 [Universidad del valle](http://www.univalle.edu.co/). 
En grupos de 2 estudiantes deben elegir una de las funciones de álgebra para implementar y uno de los métodos de Scan. Es decir que una de los métodos de Scan se implementará por 2 grupos.
Luego 1 grupo de 4 estudiantes implementarán el ejecutor de consultas, es decir, integrarán las funciones implementadas en la fase anterior, para responder una consulta que se dá como un árbol de operaciones físicas.
El otro grupo de 4 estudiantes, implementará el parser de las consultas en SQL al árbol lógico y luego al árbol de operaciones físicas.



## Definición de los datos
Dados las 2 tablas de la base de datos A y B, cada una almacenada como un directorio con el nombre de la tabla y compuesta por varios archivos:
 - schema: Esquema de la tabla, es un archivo de una sola fila, donde 	cada columna contiene una cadena de caracteres de la forma nombre-tipo-index, donde nombre hace referencia al nombre de la columna, tipo hace referencia a su tipo de dato, que puede ser “number”, “boolean”, “string” e index puede tener 3 valores “btree” o “hash” o “”, indicando que la columna se debe indexar, usando el tipo de índice especificado. En caso de estar vacía, la columna no se debe indexar. Ejemplo:

| id(int)| b-string-hash | c-boolean  | x-number|
| -------|:-------------:| ----------:|--------:|


 - A_1.csv, A_2.csv, ... A_n.csv: Son archivos separados por comas, 	cada uno conteniendo un bloque de tuplas del tipo A. Cada tupla es una fila del archivo, y sus elementos satisfacen el orden y los tipos definidos en A.schema. Ejemplo:
 

| id(int)| b-string-hash | c-boolean  | x-number|
| -------|:-------------:| ----------:|--------:|
| 1|abc|false|1.001|
| 2|def|true|0.110|


## Parte 1  (Dividirse en 4 grupos de 2 estudiantes)

Diseñe, documente e implemente las siguientes operaciones del álgebra relacional:
 - Eliminación de repetidos: Dada una tabla R, la función produce una nueva tabla R2, sin elementos repetidos.
 - Selección: Dada una relación R y una expresión conjuntiva φ, la función produce una nueva tabla R2, tal que que todas las tuplas 	t de R2, pertenecen a R y satisfacen que φ(t) = true	
 - Proyección: Dada una relación R y una lista de atributos X, que es 	un subconjunto de los atributos de R, la función produce una nueva tabla R2, desde la tabla R, con los atributos especificados en X. La nueva tabla R2 no contiene elementos repetidos.
 - Join: Dadas 2 relaciones R y S, y un conjunto de parejas r1=s1, r2=s2, donde ri es una columna de R y si es una columna de S, la función produce una nueva tabla RS cuyas tupas son de la forma (x,y) donde x es una tupla de R e y es una tupla de S. Si las relaciones R y S tienen atributos w con el mismo nombre los correspondientes atributos en RS, se re-nombrarán como R.w y S.w. Note que el número de atributos de RS debe ser exactamente la suma del número de atributos de R y S.

Note que todas estas funciones requieren la implementación de la función Scan, que lee una tabla desde el disco duro. La función Scan tiene al menos 3 versiones:
 - Seq-Scan: Lee los datos de la tabla de forma secuencial, un bloque a la vez, una tupla a la vez
 - Index-Scan: Usa algún índice para leer tupla por tupla, toda la tabla.
 - Sort-Scan: Lee los datos de forma secuencial, pero luego los ordena según algún criterio. Es decir que esta tabla produce una tabla intermedia cuyas tuplas tienen 	algún orden especificado

Cuando se hace un sort scan y la columna especificada no está indexada, entonces los datos numéricos se deben indexar usando árboles y los datos de texto con tablas Hash.

## Parte 2 (En 2 grupos de 4 estudiantes)

Usando las funciones de la parte 1, se debe implementar un motor de ejecución de consultas. El motor debe tener al menos la implementación del parser, que convierte una consulta SQL en un árbol de operaciones básicas (SFW) y luego en un árbol de operaciones físicas. 
El lenguaje estará limitado por las operaciones que se implementarán, es decir que será una versión reducida y simplista del lenguaje de consultas SQL implementado en MySQL o PostgreSQL. 
La aplicación debe estar en capacidad de resolver las siguientes consulta:

 - A(id, b, c, x)
 - B(id, m, a, price)
 
```sql	
	SELECT id, b FROM A WHERE c=true;
	SELECT A.id, price FROM A, B WHERE c=true AND A.id =a;
```
