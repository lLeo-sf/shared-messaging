using Azure.Messaging.ServiceBus;
using Messaging.Domain;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Messaging.Adapters.PubSub.AzureServiceBus;

public class Publisher : IPublisher
{
    private readonly ServiceBusClient _azServiceBusClient;
    private readonly ILogger<Publisher> _logger;
    private readonly string? _azServiceBusConnectionString;

    public Publisher(IConfiguration configuration, ILogger<Publisher> logger)
    {
        _azServiceBusConnectionString = configuration.GetConnectionString("AzureServiceBus")
            ?? throw new ArgumentException(nameof(_azServiceBusConnectionString));

        _azServiceBusClient = new ServiceBusClient(_azServiceBusConnectionString);
        _logger = logger;
    }


    public async Task SendMessageAsync(string message, string topicOrQueueName,
        string? partitionId, CancellationToken cancelToken)
    {
        ServiceBusSender sender = _azServiceBusClient.CreateSender(topicOrQueueName);
        ServiceBusMessage azMessage = new(message);

        if (!string.IsNullOrEmpty(partitionId))
        {
            azMessage.SessionId = partitionId;
        }

        await sender.SendMessageAsync(azMessage, cancelToken);

        _logger.LogInformation("Message sent to {TopicOrQueueName}\n", topicOrQueueName);
    }

    public async ValueTask DisposeAsync()
    {
        await _azServiceBusClient.DisposeAsync();
        GC.SuppressFinalize(this);
    }
}