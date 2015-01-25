# Project-mapreduce-FindFrequentItemset
Implemented SON algorithm to find frequent itemsets in transcation file.


###interface: AprioriMR.java

Usage: AprioriMR &lt;input_dir&gt; &lt;tmp_dir&gt; &lt;out_dir&gt; &lt;NumberofLinesPerRecord&gt; &lt;support(percentage)&gt;

###main classes:


* Apriori.class: in-memory Apriori algorithm.
* MyNLinesInputFormat.class and MyNLinesRecordReader.class: override default inputformat and recordreader class in mapreduce. make sure map task read custmized lines of input as one record.
* SplitFile.class: split one transcation file(.txt) into certain files as required to feed the map-reduce by its default filesplit input split scheme.
