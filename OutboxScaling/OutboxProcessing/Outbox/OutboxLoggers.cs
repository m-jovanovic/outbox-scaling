namespace OutboxProcessing.Outbox;

internal static partial class OutboxLoggers
{
    [LoggerMessage(Level = LogLevel.Information, Message = "OutboxBackgroundService starting...")]
    internal static partial void LogStarting(ILogger logger);

    [LoggerMessage(Level = LogLevel.Information, Message = "Starting iteration {IterationCount}")]
    internal static partial void LogStartingIteration(ILogger logger, int iterationCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "Iteration {IterationCount} completed. Processed {ProcessedMessages} messages. Total processed: {TotalProcessedMessages}")]
    internal static partial void LogIterationCompleted(ILogger logger, int iterationCount, int processedMessages, int totalProcessedMessages);

    [LoggerMessage(Level = LogLevel.Information, Message = "OutboxBackgroundService operation cancelled.")]
    internal static partial void LogOperationCancelled(ILogger logger);

    [LoggerMessage(Level = LogLevel.Error, Message = "An error occurred in OutboxBackgroundService")]
    internal static partial void LogError(ILogger logger, Exception exception);

    [LoggerMessage(Level = LogLevel.Information, Message = "OutboxBackgroundService finished. Total iterations: {IterationCount}, Total processed messages: {TotalProcessedMessages}")]
    internal static partial void LogFinished(ILogger logger, int iterationCount, int totalProcessedMessages);

    [LoggerMessage(Level = LogLevel.Information, Message = "Outbox processing completed. Total time: {TotalTime}ms, Query time: {QueryTime}ms, Publish time: {PublishTime}ms, Update time: {UpdateTime}ms, Messages processed: {MessageCount}")]
    internal static partial void LogProcessingPerformance(ILogger logger, long totalTime, long queryTime, long publishTime, long updateTime, int messageCount);
}