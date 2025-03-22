# azure-dlq-retriever
Application to retrieve DLQ messages from Azure Service Bus and save it in a CSV file

This application will take multiple topics, subscription as input in TopicSubscriptions variable.
For each topic and subscription in the input, the application will iterate through and read the DLQ for them, add the records in CSV and create a file named deadlettermessages.csv.