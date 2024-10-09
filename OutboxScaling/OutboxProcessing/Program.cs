using MassTransit;
using Npgsql;
using OutboxProcessing;
using OutboxProcessing.Outbox;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddScoped<OutboxProcessor>();
builder.Services.AddSingleton<DatabaseInitializer>();

builder.Services.AddSingleton(_ =>
{
    var connectionString = builder.Configuration.GetConnectionString("Database");
    return new NpgsqlDataSourceBuilder(connectionString).Build();
});

builder.Services.AddMassTransit(x =>
{
    x.UsingRabbitMq((context, cfg) =>
    {
        cfg.Host(builder.Configuration.GetConnectionString("Queue"), hostCfg =>
        {
            hostCfg.ConfigureBatchPublish(batch =>
            {
                batch.Enabled = true;
            });
        });
        
        cfg.ConfigureEndpoints(context);
    });
});

builder.Services.AddHostedService<OutboxBackgroundService>();

var host = builder.Build();

await host.Services.GetRequiredService<DatabaseInitializer>().Execute();

host.Run();
