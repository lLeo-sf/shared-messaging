using Azure.Messaging.ServiceBus;
using Messaging.Domain;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Messaging.Adapters.PubSub.AzureServiceBus;

public class Subscriber : ISubscriber
{
    private readonly ILogger<Subscriber> _logger;
    private readonly string? _azServiceBusConnectionString;

    private readonly ServiceBusClient _serviceBusClient;
    private ServiceBusSessionProcessor? _sessionProcessor;
    private ServiceBusProcessor? _processor;

    public Subscriber(IConfiguration configuration, ILogger<Subscriber> logger)
    {
        _azServiceBusConnectionString = configuration.GetConnectionString("AzureServiceBus")
            ?? throw new ArgumentException(nameof(_azServiceBusConnectionString));

        var clientOptions = new ServiceBusClientOptions
        {
            RetryOptions = new ServiceBusRetryOptions
            {
                Delay = TimeSpan.FromSeconds(5),
                MaxRetries = 7,
                MaxDelay = TimeSpan.FromMinutes(7),
                Mode = ServiceBusRetryMode.Exponential,
            }
        };

        _serviceBusClient = new ServiceBusClient(_azServiceBusConnectionString, clientOptions);
        _logger = logger;
    }

    public async Task SubscribeAsync(string topicOrQueueName, string consumerGroup,
        string? partitionId, Func<EventArgs, Task> processMessageHandler,
        Func<EventArgs, Task> processErrorHandler, CancellationToken cancelToken)
    {
        if (!string.IsNullOrEmpty(partitionId))
        {
            CreateSessionProcessor(_serviceBusClient, topicOrQueueName,
                consumerGroup);

            ConfigureSessionProcessor(
                async args =>
                {
                    _logger.LogInformation("Receiving message");
                    await processMessageHandler(args);
                },
                async args =>
                {
                    _logger.LogError("Error on receive message");
                    await processErrorHandler(args);
                });

            await _sessionProcessor!.StartProcessingAsync(cancelToken);
        }
        else
        {
            CreateProcessor(_serviceBusClient, topicOrQueueName, consumerGroup);
            ConfigureProcessor(
                async args => await processMessageHandler(args),
                async args => await processErrorHandler(args));

            await _processor!.StartProcessingAsync();
        }
    }

    private void CreateSessionProcessor(ServiceBusClient client, string topicOrQueueName,
         string subscriptionName)
    {
        var sessionProcessorOptions = new ServiceBusSessionProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCallsPerSession = 1,
            MaxConcurrentSessions = 8,

        };

        _sessionProcessor = client.CreateSessionProcessor(topicOrQueueName, subscriptionName, sessionProcessorOptions);

    }
    private void CreateProcessor(ServiceBusClient client, string topicOrQueueName,
        string subscriptionName)
    {
        var processorOptions = new ServiceBusProcessorOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 1,
        };

        _processor = client.CreateProcessor(topicOrQueueName, subscriptionName, processorOptions);
    }
    private void ConfigureSessionProcessor(Func<ProcessSessionMessageEventArgs, Task> processMessageHandler, Func<ProcessErrorEventArgs, Task> processErrorHandler)
    {
        _sessionProcessor!.ProcessMessageAsync += processMessageHandler;
        _sessionProcessor!.ProcessErrorAsync += processErrorHandler;
    }
    private void ConfigureProcessor(Func<ProcessMessageEventArgs, Task> processMessageHandler, Func<ProcessErrorEventArgs, Task> processErrorHandler)
    {
        _processor!.ProcessMessageAsync += processMessageHandler;
        _processor!.ProcessErrorAsync += processErrorHandler;
    }

    async ValueTask IAsyncDisposable.DisposeAsync()
    {
        if (_processor != null)
        {
            await _processor.StopProcessingAsync();
            await _processor.DisposeAsync();
        }

        if (_sessionProcessor != null)
        {
            await _sessionProcessor.StopProcessingAsync();
            await _sessionProcessor.DisposeAsync();
        }
      ;
        await _serviceBusClient.DisposeAsync();

        GC.SuppressFinalize(this);
    }

}