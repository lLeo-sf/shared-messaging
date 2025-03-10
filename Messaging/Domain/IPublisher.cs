namespace Messaging.Domain;

public interface IPublisher : IAsyncDisposable
{
    Task SendMessageAsync(string message, string topicOrQueueName,
        string? partitionId, CancellationToken cancelToken);
}