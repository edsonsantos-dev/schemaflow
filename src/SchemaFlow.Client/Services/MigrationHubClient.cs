using Microsoft.AspNetCore.SignalR.Client;
using SchemaFlow.Shared.Contracts;

namespace SchemaFlow.Client.Services;

public sealed class MigrationHubClient : IAsyncDisposable
{
    private readonly ApiEndpointOptions _options;
    private HubConnection? _connection;

    public MigrationHubClient(ApiEndpointOptions options)
    {
        _options = options;
    }

    public event Func<MigrationProgressEvent, Task>? ProgressReceived;

    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        if (_connection is null)
        {
            var hubUri = new Uri(_options.BaseUri, "/hubs/migration");

            _connection = new HubConnectionBuilder()
                .WithUrl(hubUri)
                .WithAutomaticReconnect()
                .Build();

            _connection.On<MigrationProgressEvent>("migrationProgress", async progress =>
            {
                var handler = ProgressReceived;
                if (handler is not null)
                {
                    await handler.Invoke(progress);
                }
            });
        }

        if (_connection.State == HubConnectionState.Connected)
        {
            return;
        }

        await _connection.StartAsync(cancellationToken);
    }

    public async Task JoinJobAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        if (_connection is null)
        {
            throw new InvalidOperationException("Conexao SignalR nao iniciada.");
        }

        await _connection.InvokeAsync("JoinJobGroup", jobId, cancellationToken);
    }

    public async Task LeaveJobAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        if (_connection is null || _connection.State != HubConnectionState.Connected)
        {
            return;
        }

        await _connection.InvokeAsync("LeaveJobGroup", jobId, cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
        }
    }
}
