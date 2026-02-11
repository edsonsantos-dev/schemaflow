using SchemaFlow.Api.Hubs;
using SchemaFlow.Api.Services;
using SchemaFlow.Shared.Contracts;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();
builder.Services.AddCors(options =>
{
    options.AddPolicy("ClientPolicy", policy =>
    {
        policy
            .AllowAnyMethod()
            .AllowAnyHeader()
            .SetIsOriginAllowed(_ => true)
            .AllowCredentials();
    });
});

builder.Services.AddSignalR();

builder.Services.AddSingleton<PostgresMetadataService>();
builder.Services.AddSingleton<ValidationService>();
builder.Services.AddSingleton<StructureProvisioningService>();
builder.Services.AddSingleton<MigrationOrchestrator>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();
app.UseCors("ClientPolicy");

var api = app.MapGroup("/api");

api.MapPost("/connection/test", async (
    ConnectionTestRequest request,
    PostgresMetadataService metadataService,
    CancellationToken cancellationToken) =>
{
    if (string.IsNullOrWhiteSpace(request.ConnectionString))
    {
        return Results.BadRequest(new ConnectionTestResponse(false, "Connection string obrigatoria."));
    }

    var result = await metadataService.TestConnectionAsync(request.ConnectionString, cancellationToken);
    return Results.Ok(result);
});

api.MapPost("/discovery/schemas", async (
    SourceConnectionRequest request,
    PostgresMetadataService metadataService,
    CancellationToken cancellationToken) =>
{
    if (string.IsNullOrWhiteSpace(request.SourceConnectionString))
    {
        return Results.BadRequest("Connection string da origem e obrigatoria.");
    }

    var schemas = await metadataService.GetSchemasAsync(request.SourceConnectionString, cancellationToken);
    return Results.Ok(new SchemaDiscoveryResponse(schemas));
});

api.MapPost("/discovery/tables", async (
    TableDiscoveryRequest request,
    PostgresMetadataService metadataService,
    CancellationToken cancellationToken) =>
{
    if (string.IsNullOrWhiteSpace(request.SourceConnectionString))
    {
        return Results.BadRequest("Connection string da origem e obrigatoria.");
    }

    var schemas = request.Schemas?.Where(schema => !string.IsNullOrWhiteSpace(schema)).Distinct().ToArray() ?? [];
    var tables = await metadataService.GetTablesAsync(request.SourceConnectionString, schemas, cancellationToken);

    return Results.Ok(new TableDiscoveryResponse(tables));
});

api.MapPost("/validation/plan", async (
    ValidationRequest request,
    ValidationService validationService,
    CancellationToken cancellationToken) =>
{
    if (request.Tables.Count == 0)
    {
        return Results.BadRequest("Selecione ao menos uma tabela para validar.");
    }

    try
    {
        var response = await validationService.BuildValidationPlanAsync(request, cancellationToken);
        return Results.Ok(response);
    }
    catch (Exception ex)
    {
        return Results.BadRequest(ex.Message);
    }
});

api.MapPost("/validation/provision", async (
    StructureProvisionRequest request,
    StructureProvisioningService provisioningService,
    CancellationToken cancellationToken) =>
{
    var response = await provisioningService.ProvisionAsync(request, cancellationToken);
    return Results.Ok(response);
});

api.MapPost("/migration/jobs", (
    MigrationStartRequest request,
    MigrationOrchestrator orchestrator) =>
{
    if (request.Tables.Count == 0)
    {
        return Results.BadRequest("Selecione ao menos uma tabela para migrar.");
    }

    if (request.MaxParallelism < 1 || request.MaxParallelism > 16)
    {
        return Results.BadRequest("Paralelismo invalido. Use valores entre 1 e 16.");
    }

    var response = orchestrator.StartJob(request);
    return Results.Ok(response);
});

api.MapGet("/migration/jobs/{jobId:guid}", (Guid jobId, MigrationOrchestrator orchestrator) =>
{
    var snapshot = orchestrator.GetSnapshot(jobId);
    return snapshot is null ? Results.NotFound() : Results.Ok(snapshot);
});

api.MapGet("/migration/jobs", (MigrationOrchestrator orchestrator) =>
{
    return Results.Ok(orchestrator.GetRecentSnapshots());
});

app.MapHub<MigrationHub>("/hubs/migration");

app.Run();
