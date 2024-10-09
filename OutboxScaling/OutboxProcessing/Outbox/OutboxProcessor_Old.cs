using System.Collections.Concurrent;
using System.Diagnostics;
using Dapper;
using MassTransit;
using Npgsql;
using System.Text.Json;

namespace OutboxProcessing.Outbox;

internal sealed class OutboxProcessor_Old(
    NpgsqlDataSource dataSource,
    IPublishEndpoint publishEndpoint,
    ILogger<OutboxProcessor_Old> logger)
{
    private const int BatchSize = 1000;
    private static readonly ConcurrentDictionary<string, Type> TypeCache = new();

    public async Task<int> Execute(CancellationToken cancellationToken = default)
    {
        var totalStopwatch = Stopwatch.StartNew();
        var stepStopwatch = new Stopwatch();

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        stepStopwatch.Restart();
        var messages = (await connection.QueryAsync<OutboxMessage>(
            """
            SELECT *
            FROM outbox_messages
            WHERE processed_on_utc IS NULL
            ORDER BY occurred_on_utc LIMIT @BatchSize
            """,
            new { BatchSize },
            transaction: transaction)).AsList();
        var queryTime = stepStopwatch.ElapsedMilliseconds;

        var updateQueue = new ConcurrentQueue<OutboxUpdate>();

        stepStopwatch.Restart();
        foreach (var message in messages)
        {
            try
            {
                var messageType = Messaging.Contracts.AssemblyReference.Assembly.GetType(message.Type);
                var deserializedMessage = JsonSerializer.Deserialize(message.Content, messageType);

                await publishEndpoint.Publish(deserializedMessage, messageType, cancellationToken);

                updateQueue.Enqueue(new OutboxUpdate
                {
                    Id = message.Id,
                    ProcessedOnUtc = DateTime.UtcNow
                });
            }
            catch (Exception ex)
            {
                updateQueue.Enqueue(new OutboxUpdate
                {
                    Id = message.Id,
                    ProcessedOnUtc = DateTime.UtcNow,
                    Error = ex.ToString()
                });
            }
        }
        var publishTime = stepStopwatch.ElapsedMilliseconds;

        stepStopwatch.Restart();
        foreach (var outboxUpdate in updateQueue)
        {
            await connection.ExecuteAsync(
                """
                UPDATE outbox_messages
                SET processed_on_utc = @ProcessedOnUtc, error = @Error
                WHERE id = @Id
                """,
                outboxUpdate,
                transaction: transaction);
        }
        var updateTime = stepStopwatch.ElapsedMilliseconds;

        await transaction.CommitAsync(cancellationToken);

        totalStopwatch.Stop();
        var totalTime = totalStopwatch.ElapsedMilliseconds;

        OutboxLoggers.LogProcessingPerformance(logger, totalTime, queryTime, publishTime, updateTime, messages.Count);

        return messages.Count;
    }

    private struct OutboxUpdate
    {
        public Guid Id { get; init; }
        public DateTime ProcessedOnUtc { get; init; }
        public string? Error { get; init; }
    }
}
