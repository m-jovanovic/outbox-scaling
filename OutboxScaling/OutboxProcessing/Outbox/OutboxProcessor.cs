using System.Collections.Concurrent;
using System.Diagnostics;
using Dapper;
using MassTransit;
using Npgsql;
using System.Text.Json;

namespace OutboxProcessing.Outbox;

internal sealed class OutboxProcessor(
    NpgsqlDataSource dataSource,
    IPublishEndpoint publishEndpoint,
    ILogger<OutboxProcessor> logger)
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
            SELECT id AS Id, type AS Type, content AS Content
            FROM outbox_messages
            WHERE processed_on_utc IS NULL
            ORDER BY occurred_on_utc LIMIT @BatchSize
            FOR UPDATE SKIP LOCKED
            """,
            new { BatchSize },
            transaction: transaction)).AsList();
        var queryTime = stepStopwatch.ElapsedMilliseconds;

        var updateQueue = new ConcurrentQueue<OutboxUpdate>();

        stepStopwatch.Restart();
        var publishTasks = messages
            .Select(message => PublishMessage(message, updateQueue, publishEndpoint, cancellationToken))
            .ToList();

        await Task.WhenAll(publishTasks);
        var publishTime = stepStopwatch.ElapsedMilliseconds;

        stepStopwatch.Restart();
        if (!updateQueue.IsEmpty)
        {
            var updateSql =
                """
                UPDATE outbox_messages
                SET processed_on_utc = v.processed_on_utc,
                    error = v.error
                FROM (VALUES
                    {0}
                ) AS v(id, processed_on_utc, error)
                WHERE outbox_messages.id = v.id::uuid
                """;

            var updates = updateQueue.ToList();
            var valuesList = string.Join(",",
                updateQueue.Select((_, i) => $"(@Id{i}, @ProcessedOn{i}, @Error{i})"));

            var parameters = new DynamicParameters();

            for (int i = 0; i < updateQueue.Count; i++)
            {
                parameters.Add($"Id{i}", updates[i].Id.ToString());
                parameters.Add($"ProcessedOn{i}", updates[i].ProcessedOnUtc);
                parameters.Add($"Error{i}", updates[i].Error);
            }

            var formattedSql = string.Format(updateSql, valuesList);

            await connection.ExecuteAsync(formattedSql, parameters, transaction: transaction);
        }
        var updateTime = stepStopwatch.ElapsedMilliseconds;

        await transaction.CommitAsync(cancellationToken);

        totalStopwatch.Stop();
        var totalTime = totalStopwatch.ElapsedMilliseconds;

        OutboxLoggers.LogProcessingPerformance(logger, totalTime, queryTime, publishTime, updateTime, messages.Count);

        return messages.Count;
    }

    private static async Task PublishMessage(
        OutboxMessage message,
        ConcurrentQueue<OutboxUpdate> updateQueue,
        IPublishEndpoint publishEndpoint,
        CancellationToken cancellationToken)
    {
        try
        {
            var messageType = GetOrAddMessageType(message.Type);
            var deserializedMessage = JsonSerializer.Deserialize(message.Content, messageType)!;

            await publishEndpoint.Publish(deserializedMessage, cancellationToken);

            updateQueue.Enqueue(new OutboxUpdate { Id = message.Id, ProcessedOnUtc = DateTime.UtcNow });
        }
        catch (Exception ex)
        {
            updateQueue.Enqueue(
                new OutboxUpdate { Id = message.Id, ProcessedOnUtc = DateTime.UtcNow, Error = ex.ToString() });
        }
    }

    private static Type GetOrAddMessageType(string typename)
    {
        return TypeCache.GetOrAdd(typename, name => Messaging.Contracts.AssemblyReference.Assembly.GetType(name)!);
    }

    private struct OutboxUpdate
    {
        public Guid Id { get; init; }
        public DateTime ProcessedOnUtc { get; init; }
        public string? Error { get; init; }
    }
}
