using SchemaFlow.Shared.Contracts;

namespace SchemaFlow.Api.Services;

public sealed class ValidationService
{
    private readonly PostgresMetadataService _metadataService;

    public ValidationService(PostgresMetadataService metadataService)
    {
        _metadataService = metadataService;
    }

    public async Task<ValidationResponse> BuildValidationPlanAsync(ValidationRequest request, CancellationToken cancellationToken)
    {
        var selectedTables = request.Tables
            .Distinct()
            .OrderBy(t => t.Schema, StringComparer.Ordinal)
            .ThenBy(t => t.Name, StringComparer.Ordinal)
            .ToArray();

        var selectedSchemas = selectedTables.Select(t => t.Schema).Distinct(StringComparer.Ordinal).ToArray();

        var sourceColumns = await _metadataService.GetColumnsAsync(
            request.SourceConnectionString,
            selectedTables,
            cancellationToken);

        var missingInSource = selectedTables.Where(t => !sourceColumns.ContainsKey(t)).ToArray();
        if (missingInSource.Length > 0)
        {
            var tables = string.Join(", ", missingInSource.Select(t => t.QualifiedName));
            throw new InvalidOperationException($"As seguintes tabelas nao existem na origem: {tables}");
        }

        var existingSchemas = await _metadataService.GetExistingSchemasAsync(
            request.DestinationConnectionString,
            selectedSchemas,
            cancellationToken);

        var existingTables = await _metadataService.GetExistingTablesAsync(
            request.DestinationConnectionString,
            selectedTables,
            cancellationToken);

        var destinationColumns = await _metadataService.GetColumnsAsync(
            request.DestinationConnectionString,
            existingTables.ToArray(),
            cancellationToken);

        var missingSchemas = selectedSchemas
            .Where(schema => !existingSchemas.Contains(schema))
            .OrderBy(schema => schema, StringComparer.Ordinal)
            .ToArray();

        var missingTables = selectedTables
            .Where(table => !existingTables.Contains(table))
            .ToArray();

        var plans = new List<TableValidationPlan>(selectedTables.Length);

        foreach (var table in selectedTables)
        {
            var source = sourceColumns[table]
                .OrderBy(column => column.OrdinalPosition)
                .ToArray();

            if (!existingTables.Contains(table))
            {
                plans.Add(new TableValidationPlan(
                    table,
                    DestinationTableExists: false,
                    SourceColumns: source,
                    DestinationColumns: [],
                    MigratableColumns: source.Select(c => c.Name).ToArray(),
                    IgnoredColumns: [],
                    IncompatibleColumns: []));

                continue;
            }

            var destination = destinationColumns.TryGetValue(table, out var rawDestination)
                ? rawDestination.OrderBy(column => column.OrdinalPosition).ToArray()
                : [];

            var destinationByName = destination.ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase);

            var migratable = new List<string>();
            var ignored = new List<string>();
            var incompatible = new List<ColumnCompatibilityIssue>();

            foreach (var sourceColumn in source)
            {
                if (!destinationByName.TryGetValue(sourceColumn.Name, out var destinationColumn))
                {
                    ignored.Add(sourceColumn.Name);
                    continue;
                }

                if (destinationColumn.IsGenerated)
                {
                    incompatible.Add(new ColumnCompatibilityIssue(
                        sourceColumn.Name,
                        sourceColumn.TypeDefinition,
                        destinationColumn.TypeDefinition,
                        "Coluna gerada no destino (somente leitura)."));
                    continue;
                }

                if (destinationColumn.IsIdentity)
                {
                    incompatible.Add(new ColumnCompatibilityIssue(
                        sourceColumn.Name,
                        sourceColumn.TypeDefinition,
                        destinationColumn.TypeDefinition,
                        "Coluna identity no destino sem suporte a OVERRIDING SYSTEM VALUE no fluxo atual."));
                    continue;
                }

                if (!AreTypesCompatible(sourceColumn.TypeDefinition, destinationColumn.TypeDefinition))
                {
                    incompatible.Add(new ColumnCompatibilityIssue(
                        sourceColumn.Name,
                        sourceColumn.TypeDefinition,
                        destinationColumn.TypeDefinition,
                        "Tipo de coluna incompativel."));
                    continue;
                }

                migratable.Add(sourceColumn.Name);
            }

            plans.Add(new TableValidationPlan(
                table,
                DestinationTableExists: true,
                SourceColumns: source,
                DestinationColumns: destination,
                MigratableColumns: migratable,
                IgnoredColumns: ignored,
                IncompatibleColumns: incompatible));
        }

        var requiresConfirmation = missingSchemas.Length > 0 ||
                                   missingTables.Length > 0 ||
                                   plans.Any(plan => plan.IgnoredColumns.Count > 0 || plan.IncompatibleColumns.Count > 0);

        return new ValidationResponse(missingSchemas, missingTables, plans, requiresConfirmation);
    }

    private static bool AreTypesCompatible(string sourceType, string destinationType)
    {
        static string Normalize(string value)
            => value.Trim().Replace(" ", string.Empty, StringComparison.Ordinal).ToLowerInvariant();

        return Normalize(sourceType) == Normalize(destinationType);
    }
}
