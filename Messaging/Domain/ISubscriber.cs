namespace Messaging.Domain;

public interface ISubscriber : IAsyncDisposable
{
    Task SubscribeAsync(string topicOrQueueName, string consumerGroup,
        string? partitionId, Func<EventArgs, Task> processMessageHandler,
        Func<EventArgs, Task> processErrorHandler, CancellationToken cancelToken);
}