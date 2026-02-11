using Npgsql;
using SchemaFlow.Api.Infrastructure;
using SchemaFlow.Shared.Contracts;

namespace SchemaFlow.Api.Services;

public sealed class StructureProvisioningService
{
    private readonly PostgresMetadataService _metadataService;
    private readonly ILogger<StructureProvisioningService> _logger;

    public StructureProvisioningService(
        PostgresMetadataService metadataService,
        ILogger<StructureProvisioningService> logger)
    {
        _metadataService = metadataService;
        _logger = logger;
    }

    public async Task<StructureProvisionResponse> ProvisionAsync(
        StructureProvisionRequest request,
        CancellationToken cancellationToken)
    {
        var createdSchemas = new List<string>();
        var createdTables = new List<TableIdentifier>();
        var errors = new List<string>();

        await using var destinationConnection = new NpgsqlConnection(request.DestinationConnectionString);
        await destinationConnection.OpenAsync(cancellationToken);

        foreach (var schema in request.SchemasToCreate.Distinct(StringComparer.Ordinal))
        {
            try
            {
                var sql = $"CREATE SCHEMA IF NOT EXISTS {PostgresSql.QuoteIdentifier(schema)};";
                await using var command = new NpgsqlCommand(sql, destinationConnection);
                await command.ExecuteNonQueryAsync(cancellationToken);
                createdSchemas.Add(schema);
            }
            catch (Exception ex)
            {
                var message = $"Erro ao criar schema '{schema}': {ex.Message}";
                errors.Add(message);
                _logger.LogError(ex, "{Message}", message);
            }
        }

        var tablesToCreate = request.TablesToCreate
            .Distinct()
            .OrderBy(t => t.Schema, StringComparer.Ordinal)
            .ThenBy(t => t.Name, StringComparer.Ordinal)
            .ToArray();

        if (tablesToCreate.Length == 0)
        {
            return new StructureProvisionResponse(createdSchemas, createdTables, errors);
        }

        var sourceColumns = await _metadataService.GetColumnsAsync(
            request.SourceConnectionString,
            tablesToCreate,
            cancellationToken);

        foreach (var table in tablesToCreate)
        {
            if (!sourceColumns.TryGetValue(table, out var columns) || columns.Count == 0)
            {
                var noColumnMessage = $"Tabela '{table.QualifiedName}' sem colunas na origem.";
                errors.Add(noColumnMessage);
                _logger.LogWarning("{Message}", noColumnMessage);
                continue;
            }

            try
            {
                var columnSql = string.Join(", ", columns
                    .OrderBy(column => column.OrdinalPosition)
                    .Select(column =>
                    {
                        var nullability = column.IsNullable ? string.Empty : " NOT NULL";
                        return $"{PostgresSql.QuoteIdentifier(column.Name)} {column.TypeDefinition}{nullability}";
                    }));

                var createTableSql =
                    $"CREATE TABLE IF NOT EXISTS {PostgresSql.QualifiedTable(table.Schema, table.Name)} ({columnSql});";

                await using var command = new NpgsqlCommand(createTableSql, destinationConnection);
                await command.ExecuteNonQueryAsync(cancellationToken);
                createdTables.Add(table);
            }
            catch (Exception ex)
            {
                var message = $"Erro ao criar tabela '{table.QualifiedName}': {ex.Message}";
                errors.Add(message);
                _logger.LogError(ex, "{Message}", message);
            }
        }

        return new StructureProvisionResponse(createdSchemas, createdTables, errors);
    }
}
