# HPC

## TODO
### ENV SETUP
   1) Install Java 17
   2) Install Intellij Community
   3) Install mySQL
### Project steps
   1) Get excel sheet (done, see resources folder)
   2) The data importer that's built into mysql does not support empty columns. If a flight is canceled in the sheet (the right most value) there is no arrival date and the import command in mysql workbench ignores it. Because of this we need to write a small csv to sql converter (easy)
   3) Once imported run some analysis while it's in sql to weight our graph (add some columns for each flight that show the average for the airline per airport using groupbys. This should probably just be a move to another table) while this is not good SQL hbase is based on a document model so in order to import it if we think of each row as a document and stuff everything we can into it for each flight then that will make step 4 easier (medium difficulty)
   4) use sqoop to import the mysql to hbase (easy, that's what this tool is for)
   5) Template a haddoop project and attach it to hbase (easy, copy paste)
   6) Figure out what information we want to allow input for (ie just --dest=xyz --depart=abc or do we want more flags?) 
   7) For starter let's just look at the direct flight route, create a mapper and reducer
   8) Add another mapper and reducer that works by looking at all airports returned from the first mapper and adds it as starting points for the second mapper. We will not be doing more than 1 layover for our initial build. 

