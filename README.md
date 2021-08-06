# Equi Join using MapReduce - Java and Hadoop framework

The code would take two inputs - 
* HDFS location of the input file 
* HDFS location of the file where the output should be stored

**Input file format:**

Table1Name, Table1JoinColumn, Table1Other Column1, Table1OtherColumn2, ……..

Table2Name, Table2JoinColumn, Table2Other Column1, Table2OtherColumn2, ……...

**Driver:**
* The Driver program creates an instance of the mapreduce job, setting up the mapper and the reducer configurations. It also gets the input file path to run the mapreduce job on, as well as the output file path to store the results.  

**Mapper:**
* The mapper gets a tuple (row) from the file, containing the table name, join column, and other column values of the table, as the input. It splits the row using comma as delimiter to get the join column value. Then, it takes the join column value as the key and the tuple as the value. The output of the mapper is <join column value, given tuple>.

**Reducer:**
* The reducer gets < join column value, < set of tuples with the same join column value >> as the input. It separates the tuples according to the table they belong to. Then, it matches each tuple of given table1 with each tuple of table2 from the given list and concatenates the table values as <table1 values, table2 values>. This is the output of the reducer. If the given list doesn't contain both the tables, such tuples will be ignored and will not be shown in the result. The outputs of all the reducers will be combined and saved in the output file. 


**Sample Input:**
  
R, 2, Don, Larson, Newark, 555-3221

S, 1, 33000, 10000, part1
  
S, 2, 18000, 2000, part1
  
S, 2, 20000, 1800, part1
  
R, 3, Sal, Maglite, Nutley, 555-6905
  
S, 3, 24000, 5000, part1
  
S, 4, 22000, 7000, part1
  
R, 4, Bob, Turley, Passaic, 555-8908
  

**Sample Output:**

R, 2, Don, Larson, Newark, 555-3221, S, 2, 18000, 2000, part1
  
R, 2, Don, Larson, Newark, 555-3221, S, 2, 20000, 1800, part1

R, 3, Sal, Maglite, Nutley, 555-6905, S, 3, 24000, 5000, part1

S, 4, 22000, 7000, part1, R, 4, Bob, Turley, Passaic, 555-8908

or

R, 2, Don, Larson, Newark, 555-3221, S, 2, 18000, 2000, part1

R, 2, Don, Larson, Newark, 555-3221, S, 2, 20000, 1800, part1

R, 3, Sal, Maglite, Nutley, 555-6905, S, 3, 24000, 5000, part1

R, 4, Bob, Turley, Passaic, 555-8908, S, 4, 22000, 7000, part1


