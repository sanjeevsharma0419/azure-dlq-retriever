using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Azure.Messaging.ServiceBus;
using CsvHelper;

namespace AzureSbDlqRetriever;

public class Program
{
    // Add the name of topic(s) and subscription(s) from which you want to read the DLQ messages
    private static readonly List<KeyValuePair<string, string>> TopicSubscriptions =
    [
        new KeyValuePair<string, string>("TOPIC-NAME-1","SUBSCRIPTION-NAME-1"),
        new KeyValuePair<string, string>("TOPIC-NAME-2","SUBSCRIPTION-NAME-2")
    ];
    
    // Add your Azure SB connection string value here
    private static readonly string azureSbConnectionString = "CONNECTION-STRING-OF-AZURE-SERVICE-BUS";

    // Use PeekLock mode to read the messages only
    // Use ReceiveAndDelete mode to read and delete the message from the subscription
    private static readonly string receiveMode = "PeekLock"; 

    private const string ExportFileName = "deadlettermessages.csv";

    private const int MaxMessage = 100;

    private delegate Task<IReadOnlyList<ServiceBusReceivedMessage>> GetMessages(ServiceBusReceiver receiver, long? fromSequenceNumber);

    public static void Main(string [] args)
    {
        ServiceBusClient client = new(azureSbConnectionString);
        
        TopicSubscriptions.ForEach(async topicSubscription => 
        {
            var serviceBusReceiver = client.CreateReceiver(topicSubscription.Key, topicSubscription.Value, GetServiceBusReceiverOptions(receiveMode));
            
            await WriteMessagesToCsv(serviceBusReceiver);
        });
    }

    // method to get service bus reciever options, which would include the recieve mode
    public static ServiceBusReceiverOptions GetServiceBusReceiverOptions(string recieveMode)
    {
        if (!string.IsNullOrEmpty(recieveMode) && Enum.TryParse<ServiceBusReceiveMode>(recieveMode, out ServiceBusReceiveMode mode))
        {
            return new ServiceBusReceiverOptions
            {
                SubQueue = SubQueue.DeadLetter,
                ReceiveMode  = ServiceBusReceiveMode.PeekLock 
            };
        }

        throw new InvalidOperationException("ServiceBusReceiveMode can only be either among PeekLock or ReceiveAndDelete mode");
    }

    private static async Task WriteMessagesToCsv(ServiceBusReceiver serviceBusReceiver)
    {
        await using var writer = new StreamWriter(ExportFileName);
        await using var csvFile = new CsvWriter(writer, CultureInfo.InvariantCulture);
        csvFile.WriteHeader<DeadLetterMessage>();
        await csvFile.NextRecordAsync();

        await foreach(DeadLetterMessage deadLetterMessage in ReadDlqMessages(serviceBusReceiver))
        {
            csvFile.WriteRecord(deadLetterMessage);
            await csvFile.NextRecordAsync();
        }
    }

    private static async IAsyncEnumerable<DeadLetterMessage> ReadDlqMessages(ServiceBusReceiver receiver)
    {
        Console.WriteLine($"Reading DLQ messages from Topic {receiver.EntityPath} using Mode {receiver.ReceiveMode}");

        GetMessages getMessages = receiver.ReceiveMode == ServiceBusReceiveMode.PeekLock 
        ? (busReceiver, fromSequenceNumber) => busReceiver.PeekMessagesAsync(MaxMessage, fromSequenceNumber)
        : (busReceiver, _) => busReceiver.ReceiveMessagesAsync(MaxMessage);
        
        IReadOnlyList<ServiceBusReceivedMessage> messages = await getMessages(receiver, 0);

        long totalMessagesRead = 0;

        while(messages.Count > 0)
        {
            ConcurrentBag<DeadLetterMessage> deadLetterMessages = [];

            Parallel.ForEach(messages, message => 
            {
                deadLetterMessages.Add(new DeadLetterMessage
                {
                    EntityPath = receiver.EntityPath,
                    MessageId = message.MessageId,
                    CorrelationId = message.CorrelationId,
                    DeliveryCount = message.DeliveryCount,
                    EnqueuedTime = message.EnqueuedTime,
                    ExceptionType = message.ApplicationProperties.ContainsKey("MT-Fault-ExceptionType") ? message.ApplicationProperties["MT-Fault-ExceptionType"].ToString()! : string.Empty,
                    ErrorMessage = message.ApplicationProperties.ContainsKey("MT-Fault-Message") ? message.ApplicationProperties["MT-Fault-Message"].ToString()! : string.Empty,
                    MessageBody = Encoding.ASCII.GetString(message.Body.ToArray())
                });
            });

            foreach(DeadLetterMessage deadLetterMessage in deadLetterMessages)
            {
                yield return deadLetterMessage;
            }

            totalMessagesRead += messages.Count;

            Console.WriteLine($"Read {totalMessagesRead} messages till now.");

            long fromSequenceNumber = messages[^1].SequenceNumber + 1;
            messages = await getMessages(receiver, fromSequenceNumber);
        }
    }
}