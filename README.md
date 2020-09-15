## Kafka and spring test task
The idea of this task is to create rest controller which will have 1 method that returns lag of consumer group. There is 1 test, that call rest method and validate result.  

Expectations: 
* good spring components structure
* clean and well formatted code
* maybe some corner case tests

Hint! To get bootstrap server use `System.getProperty("kafka.bootstrap.servers")`