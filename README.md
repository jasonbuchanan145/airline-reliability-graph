# HPC

## TODO
### Proposal
google doc https://docs.google.com/document/d/10UGqmwzwMYYr8GQrXeMW4NitfeeLIBd1T3N0JhH92yo/edit?usp=sharing 
Workflow pdf, see "resources" directory, it is editable here https://app.diagrams.net/#Hjasonbuchanan145%2FHPC%2Fmain%2Fresources%2Fworkflow.drawio
### ENV SETUP
   1) Install Java 17
   2) Install Intellij Community
   3) Install mySQL
### Project steps
   1) Get excel sheet (done, see resources folder)
   2) Create mysql docker container that imports the excel sheet (done_
   3) Once imported run some analysis while it's in sql to weight our graph (add some columns for each flight that show the average for the airline per airport using groupbys. This should probably just be a move to another table) while this is not good SQL hbase is based on a document model so in order to import it if we think of each row as a document and stuff everything we can into it for each flight then that will make step 4 easier (Done, see PR#2)


-> current step for devops
   4) create a docker container for hbase
   5) use sqoop to import the mysql to hbase (easy, that's what this tool is for)
   6) Template a haddoop project and attach it to hbase (easy, copy paste)

      
-> current step for development
   8) For starter let's just look at the direct flight route, create a mapper and reducer
   9) Add another mapper and reducer that works by looking at all airports returned from the first mapper and adds it as starting points for the second mapper. We will not be doing more than 1 layover for our initial build. 

