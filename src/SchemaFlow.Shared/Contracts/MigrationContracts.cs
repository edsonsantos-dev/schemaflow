namespace SchemaFlow.Shared.Contracts;

public record ConnectionTestRequest(string ConnectionString);

public record ConnectionTestResponse(
    bool Success,
    string Message,
    string? DatabaseName = null,
    string? ServerVersion = null);

public record SourceConnectionRequest(string SourceConnectionString);

public record SchemaDiscoveryResponse(IReadOnlyList<string> Schemas);

public record TableDiscoveryRequest(
    string SourceConnectionString,
    IReadOnlyList<string> Schemas);

public record TableIdentifier(string Schema, string Name)
{
    public string QualifiedName => $"{Schema}.{Name}";
}

public record TableMetadata(TableIdentifier Table, long? EstimatedRows);

public record TableDiscoveryResponse(IReadOnlyList<TableMetadata> Tables);

public record ColumnMetadata(
    string Name,
    string TypeDefinition,
    bool IsNullable,
    string? DefaultExpression,
    int OrdinalPosition,
    bool IsGenerated,
    bool IsIdentity);

public record ColumnCompatibilityIssue(
    string Column,
    string SourceType,
    string DestinationType,
    string Reason);

public record TableValidationPlan(
    TableIdentifier Table,
    bool DestinationTableExists,
    IReadOnlyList<ColumnMetadata> SourceColumns,
    IReadOnlyList<ColumnMetadata> DestinationColumns,
    IReadOnlyList<string> MigratableColumns,
    IReadOnlyList<string> IgnoredColumns,
    IReadOnlyList<ColumnCompatibilityIssue> IncompatibleColumns);

public record ValidationRequest(
    string SourceConnectionString,
    string DestinationConnectionString,
    IReadOnlyList<TableIdentifier> Tables);

public record ValidationResponse(
    IReadOnlyList<string> MissingSchemas,
    IReadOnlyList<TableIdentifier> MissingTables,
    IReadOnlyList<TableValidationPlan> TablePlans,
    bool RequiresConfirmation);

public record StructureProvisionRequest(
    string SourceConnectionString,
    string DestinationConnectionString,
    IReadOnlyList<string> SchemasToCreate,
    IReadOnlyList<TableIdentifier> TablesToCreate);

public record StructureProvisionResponse(
    IReadOnlyList<string> CreatedSchemas,
    IReadOnlyList<TableIdentifier> CreatedTables,
    IReadOnlyList<string> Errors);

public enum MigrationLoadMode
{
    Append = 0,
    TruncateBeforeLoad = 1
}

public record TableMigrationRequest(
    TableIdentifier Table,
    IReadOnlyList<string> ColumnsToMigrate,
    long? EstimatedRows);

public record MigrationStartRequest(
    string SourceConnectionString,
    string DestinationConnectionString,
    IReadOnlyList<TableMigrationRequest> Tables,
    int MaxParallelism,
    MigrationLoadMode LoadMode);

public record MigrationStartResponse(Guid JobId);

public enum MigrationJobStatus
{
    Pending = 0,
    Running = 1,
    Completed = 2,
    CompletedWithErrors = 3,
    Failed = 4,
    Canceled = 5
}

public enum TableMigrationStatus
{
    Pending = 0,
    Running = 1,
    Retrying = 2,
    Completed = 3,
    Failed = 4,
    Skipped = 5
}

public enum TableMigrationPhase
{
    Pending = 0,
    Preparing = 1,
    ReadingSource = 2,
    WritingDestination = 3,
    CompletingCopy = 4,
    Committing = 5,
    Retrying = 6,
    Failed = 7,
    Completed = 8,
    Skipped = 9
}

public record TableProgressSnapshot(
    TableIdentifier Table,
    TableMigrationStatus Status,
    TableMigrationPhase Phase,
    int Attempt,
    long ProcessedRows,
    long? EstimatedRows,
    double ProgressPercent,
    double RowsPerSecond,
    string? Message,
    DateTimeOffset UpdatedAt);

public record MigrationJobSnapshot(
    Guid JobId,
    MigrationJobStatus Status,
    DateTimeOffset StartedAt,
    DateTimeOffset? FinishedAt,
    int TotalTables,
    int CompletedTables,
    int FailedTables,
    long ProcessedRows,
    long EstimatedRows,
    double ThroughputRowsPerSecond,
    IReadOnlyList<TableProgressSnapshot> Tables,
    string? ErrorMessage);

public record MigrationProgressEvent(
    Guid JobId,
    MigrationJobStatus JobStatus,
    TableProgressSnapshot? Table,
    int CompletedTables,
    int TotalTables,
    int FailedTables,
    long ProcessedRows,
    long EstimatedRows,
    double ThroughputRowsPerSecond,
    DateTimeOffset Timestamp);
