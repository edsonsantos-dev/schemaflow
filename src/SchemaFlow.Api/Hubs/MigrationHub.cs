using Microsoft.AspNetCore.SignalR;

namespace SchemaFlow.Api.Hubs;

public sealed class MigrationHub : Hub
{
    public Task JoinJobGroup(Guid jobId)
        => Groups.AddToGroupAsync(Context.ConnectionId, jobId.ToString("N"));

    public Task LeaveJobGroup(Guid jobId)
        => Groups.RemoveFromGroupAsync(Context.ConnectionId, jobId.ToString("N"));
}
