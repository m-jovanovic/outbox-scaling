namespace OutboxProcessing.Outbox;

internal class OutboxBackgroundService(
    IServiceScopeFactory serviceScopeFactory,
    ILogger<OutboxBackgroundService> logger) : BackgroundService
{
    private const int OutboxProcessorFrequency = 5;
    private readonly int _maxParallelism = 5;
    private int _totalIterations = 0;
    private int _totalProcessedMessage = 0;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        OutboxLoggers.LogStarting(logger);

        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, stoppingToken);

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = _maxParallelism,
            CancellationToken = linkedCts.Token
        };

        try
        {
            await Parallel.ForEachAsync(
                Enumerable.Range(0, _maxParallelism),
                parallelOptions,
                async (_, token) =>
                {
                    await ProcessOutboxMessages(token);
                });
        }
        catch (OperationCanceledException)
        {
            OutboxLoggers.LogOperationCancelled(logger);
        }
        catch (Exception ex)
        {
            OutboxLoggers.LogError(logger, ex);
        }
        finally
        {
            OutboxLoggers.LogFinished(logger, _totalIterations, _totalProcessedMessage);
        }
    }

    private async Task ProcessOutboxMessages(CancellationToken cancellationToken)
    {
        using var scope = serviceScopeFactory.CreateScope();
        var outboxProcessor = scope.ServiceProvider.GetRequiredService<OutboxProcessor>();

        while (!cancellationToken.IsCancellationRequested)
        {
            var iterationCount = Interlocked.Increment(ref _totalIterations);
            OutboxLoggers.LogStartingIteration(logger, iterationCount);

            int processedMessages = await outboxProcessor.Execute(cancellationToken);
            var totalProcessedMessages = Interlocked.Add(ref _totalProcessedMessage, processedMessages);

            OutboxLoggers.LogIterationCompleted(logger, iterationCount, processedMessages, totalProcessedMessages);

            // Simulate running Outbox processing every N seconds
            //await Task.Delay(TimeSpan.FromSeconds(OutboxProcessorFrequency), cancellationToken);
        }
    }
}