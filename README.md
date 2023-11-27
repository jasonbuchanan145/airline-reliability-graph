# HPC Airline Reliability Evaluation System

## Summary
A project to report on airline reliability between any two airports reported to the Beuru of Transportation Statistics using Spark, Spring Boot, MySQL and Docker

### Workflow diagram
[see this pdf](References/workflow.drawio.pdf)

### Original Proposal
google doc https://docs.google.com/document/d/10UGqmwzwMYYr8GQrXeMW4NitfeeLIBd1T3N0JhH92yo/edit?usp=sharing 

### Run
1) Install docker desktop and start the application which will start the docker machine
2) Go to the ./HPC/services
3) Run docker-compose up
4) Wait for build and intialization to complete this can take several minutes as maven downloads dependencies

### Project Directory Layout
1) /services the main directory of this project.
2) /services/database contains both the MySQL components for database to preweight our graph
3) /services/spark the spring boot application using spark to aggregate the graph and provide a web ui
4) /References/ papers and various documents for this project
5) /resources/ deprecated directory
