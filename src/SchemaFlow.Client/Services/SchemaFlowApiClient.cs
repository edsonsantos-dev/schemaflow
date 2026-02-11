using System.Net.Http.Json;
using SchemaFlow.Shared.Contracts;

namespace SchemaFlow.Client.Services;

public sealed class SchemaFlowApiClient
{
    private readonly HttpClient _httpClient;

    public SchemaFlowApiClient(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public async Task<ConnectionTestResponse> TestConnectionAsync(
        string connectionString,
        CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsJsonAsync(
            "/api/connection/test",
            new ConnectionTestRequest(connectionString),
            cancellationToken);

        await EnsureSuccessAsync(response, cancellationToken);
        return (await response.Content.ReadFromJsonAsync<ConnectionTestResponse>(cancellationToken))!;
    }

    public async Task<SchemaDiscoveryResponse> DiscoverSchemasAsync(
        string sourceConnectionString,
        CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsJsonAsync(
            "/api/discovery/schemas",
            new SourceConnectionRequest(sourceConnectionString),
            cancellationToken);

        await EnsureSuccessAsync(response, cancellationToken);
        return (await response.Content.ReadFromJsonAsync<SchemaDiscoveryResponse>(cancellationToken))!;
    }

    public async Task<TableDiscoveryResponse> DiscoverTablesAsync(
        string sourceConnectionString,
        IReadOnlyList<string> schemas,
        CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsJsonAsync(
            "/api/discovery/tables",
            new TableDiscoveryRequest(sourceConnectionString, schemas),
            cancellationToken);

        await EnsureSuccessAsync(response, cancellationToken);
        return (await response.Content.ReadFromJsonAsync<TableDiscoveryResponse>(cancellationToken))!;
    }

    public async Task<ValidationResponse> BuildValidationPlanAsync(
        ValidationRequest request,
        CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsJsonAsync(
            "/api/validation/plan",
            request,
            cancellationToken);

        await EnsureSuccessAsync(response, cancellationToken);
        return (await response.Content.ReadFromJsonAsync<ValidationResponse>(cancellationToken))!;
    }

    public async Task<StructureProvisionResponse> ProvisionAsync(
        StructureProvisionRequest request,
        CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsJsonAsync(
            "/api/validation/provision",
            request,
            cancellationToken);

        await EnsureSuccessAsync(response, cancellationToken);
        return (await response.Content.ReadFromJsonAsync<StructureProvisionResponse>(cancellationToken))!;
    }

    public async Task<MigrationStartResponse> StartMigrationAsync(
        MigrationStartRequest request,
        CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.PostAsJsonAsync(
            "/api/migration/jobs",
            request,
            cancellationToken);

        await EnsureSuccessAsync(response, cancellationToken);
        return (await response.Content.ReadFromJsonAsync<MigrationStartResponse>(cancellationToken))!;
    }

    public async Task<MigrationJobSnapshot> GetMigrationSnapshotAsync(
        Guid jobId,
        CancellationToken cancellationToken = default)
    {
        var response = await _httpClient.GetAsync($"/api/migration/jobs/{jobId}", cancellationToken);
        await EnsureSuccessAsync(response, cancellationToken);
        return (await response.Content.ReadFromJsonAsync<MigrationJobSnapshot>(cancellationToken))!;
    }

    private static async Task EnsureSuccessAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
        {
            return;
        }

        var content = await response.Content.ReadAsStringAsync(cancellationToken);
        if (string.IsNullOrWhiteSpace(content))
        {
            content = response.ReasonPhrase ?? "Erro inesperado.";
        }

        throw new InvalidOperationException(content);
    }
}
