namespace AzureSbDlqRetriever;

public class DeadLetterMessage
{
    public string EntityPath { get; set; }

    public string MessageId { get; set; }

    public string CorrelationId { get; set; }

    public int DeliveryCount { get; set; }

    public DateTimeOffset EnqueuedTime { get; set; }

    public string ExceptionType { get; set; }

    public string ErrorMessage { get; set; }

    public string MessageBody { get; set; }
}