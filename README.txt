Q1:  Find  the top K authors who  rank  the  highest based  on the number  of  unique  coauthors they have.Here, K is an input argument to the query.

Q2: For each author, find the top K coauthors with whom this author has published the most, and  list  the  number  of  papers  between  this  author  and  her  coauthors.  Again, K is an  input argument to the query.

Q3:Sort  all  the  papers  by  the  venue  name  (lexicographically)  and  then  by  the  publication year. Display the first K papers in the sorted list, where K is an input argument to your query

Q1. yarn jar q1-1.0-SNAPSHOT.jar Q1 /student0_paperauths.tsv /parti1.seqfile /Q1 5

With this command, the output will be in /Q1/part-r-00000 and the sequence file used to partition will be in /parti1.seqfile.


Q2. yarn jar hw4-q2-1.0-SNAPSHOT.jar Q2 /student0_paperauths.tsv /parti2.seqfile /Q2 5

With this command, the output will be in /Q2 and the and the sequence file used to partition will be in /parti2.seqfile.


Q3. yarn jar hw4-q3-1.0-SNAPSHOT.jar Query3 /student0_venue.tsv /student0_papers.tsv /tmp /Q3 5

With this command, the output will be in /Q3.