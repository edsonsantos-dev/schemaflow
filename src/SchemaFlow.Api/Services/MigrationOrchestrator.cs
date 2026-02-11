using System.Collections.Concurrent;
using System.Data;
using System.Diagnostics;
using Microsoft.AspNetCore.SignalR;
using Npgsql;
using SchemaFlow.Api.Hubs;
using SchemaFlow.Api.Infrastructure;
using SchemaFlow.Shared.Contracts;

namespace SchemaFlow.Api.Services;

public sealed class MigrationOrchestrator
{
    private const int MaxRetryAttempts = 3;
    private readonly IHubContext<MigrationHub> _hubContext;
    private readonly ILogger<MigrationOrchestrator> _logger;
    private readonly ConcurrentDictionary<Guid, MigrationJobRuntime> _jobs = new();

    public MigrationOrchestrator(
        IHubContext<MigrationHub> hubContext,
        ILogger<MigrationOrchestrator> logger)
    {
        _hubContext = hubContext;
        _logger = logger;
    }

    public MigrationStartResponse StartJob(MigrationStartRequest request)
    {
        var job = new MigrationJobRuntime(request);

        if (!_jobs.TryAdd(job.JobId, job))
        {
            throw new InvalidOperationException("Nao foi possivel registrar o job de migracao.");
        }

        _ = Task.Run(() => RunJobAsync(job, request, CancellationToken.None));

        return new MigrationStartResponse(job.JobId);
    }

    public MigrationJobSnapshot? GetSnapshot(Guid jobId)
        => _jobs.TryGetValue(jobId, out var runtime) ? runtime.GetSnapshot() : null;

    public IReadOnlyList<MigrationJobSnapshot> GetRecentSnapshots(int limit = 30)
    {
        return _jobs.Values
            .Select(job => job.GetSnapshot())
            .OrderByDescending(job => job.StartedAt)
            .Take(limit)
            .ToArray();
    }

    private async Task RunJobAsync(MigrationJobRuntime job, MigrationStartRequest request, CancellationToken cancellationToken)
    {
        job.SetJobStatus(MigrationJobStatus.Running, null);
        await PublishProgressAsync(job, null, cancellationToken);

        var maxParallelism = Math.Clamp(request.MaxParallelism, 1, 16);
        using var semaphore = new SemaphoreSlim(maxParallelism, maxParallelism);

        var tasks = request.Tables.Select(async tableRequest =>
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                await ProcessTableWithRetryAsync(job, request, tableRequest, cancellationToken);
            }
            finally
            {
                semaphore.Release();
            }
        });

        try
        {
            await Task.WhenAll(tasks);

            var snapshot = job.GetSnapshot();
            if (snapshot.FailedTables == 0)
            {
                job.SetJobStatus(MigrationJobStatus.Completed, null);
            }
            else if (snapshot.CompletedTables > 0)
            {
                job.SetJobStatus(MigrationJobStatus.CompletedWithErrors, null);
            }
            else
            {
                job.SetJobStatus(MigrationJobStatus.Failed, "Todas as tabelas falharam.");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha na execucao do job {JobId}", job.JobId);
            job.SetJobStatus(MigrationJobStatus.Failed, ex.Message);
        }

        job.MarkFinished();
        await PublishProgressAsync(job, null, cancellationToken);
    }

    private async Task ProcessTableWithRetryAsync(
        MigrationJobRuntime job,
        MigrationStartRequest request,
        TableMigrationRequest tableRequest,
        CancellationToken cancellationToken)
    {
        if (tableRequest.ColumnsToMigrate.Count == 0)
        {
            var skipped = job.UpdateTable(
                tableRequest.Table,
                TableMigrationStatus.Skipped,
                attempt: 1,
                message: "Sem colunas compativeis para migracao.");
            await PublishProgressAsync(job, skipped, cancellationToken);
            return;
        }

        for (var attempt = 1; attempt <= MaxRetryAttempts; attempt++)
        {
            var runningStatus = job.UpdateTable(
                tableRequest.Table,
                attempt == 1 ? TableMigrationStatus.Running : TableMigrationStatus.Retrying,
                attempt,
                attempt == 1 ? "Migracao iniciada." : "Nova tentativa em execucao.");
            await PublishProgressAsync(job, runningStatus, cancellationToken);

            try
            {
                var copiedRows = await MigrateTableAsync(job, request, tableRequest, cancellationToken);

                var completed = job.MarkTableCompleted(
                    tableRequest.Table,
                    attempt,
                    copiedRows,
                    "Migracao concluida.");

                await PublishProgressAsync(job, completed, cancellationToken);
                return;
            }
            catch (Exception ex) when (attempt < MaxRetryAttempts && IsTransient(ex))
            {
                _logger.LogWarning(
                    ex,
                    "Erro transitorio na tabela {Table}. Tentativa {Attempt}/{MaxAttempts}.",
                    tableRequest.Table.QualifiedName,
                    attempt,
                    MaxRetryAttempts);

                var retrying = job.UpdateTable(
                    tableRequest.Table,
                    TableMigrationStatus.Retrying,
                    attempt,
                    $"Erro transitorio: {ex.Message}");

                await PublishProgressAsync(job, retrying, cancellationToken);
                await Task.Delay(TimeSpan.FromSeconds(2 * attempt), cancellationToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Falha definitiva na tabela {Table} apos {Attempt} tentativa(s).",
                    tableRequest.Table.QualifiedName,
                    attempt);

                var failed = job.MarkTableFailed(tableRequest.Table, attempt, ex.Message);
                await PublishProgressAsync(job, failed, cancellationToken);
                return;
            }
        }
    }

    private async Task<long> MigrateTableAsync(
        MigrationJobRuntime job,
        MigrationStartRequest request,
        TableMigrationRequest tableRequest,
        CancellationToken cancellationToken)
    {
        var columns = tableRequest.ColumnsToMigrate
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var qualifiedTable = PostgresSql.QualifiedTable(tableRequest.Table.Schema, tableRequest.Table.Name);
        var quotedColumns = string.Join(", ", columns.Select(PostgresSql.QuoteIdentifier));

        var selectSql = $"SELECT {quotedColumns} FROM {qualifiedTable};";
        var copySql = $"COPY {qualifiedTable} ({quotedColumns}) FROM STDIN (FORMAT BINARY);";

        await using var sourceConnection = new NpgsqlConnection(request.SourceConnectionString);
        await sourceConnection.OpenAsync(cancellationToken);

        await using var destinationConnection = new NpgsqlConnection(request.DestinationConnectionString);
        await destinationConnection.OpenAsync(cancellationToken);

        await using var transaction = await destinationConnection.BeginTransactionAsync(cancellationToken);

        try
        {
            if (request.LoadMode == MigrationLoadMode.TruncateBeforeLoad)
            {
                var truncateSql = $"TRUNCATE TABLE {qualifiedTable};";
                await using var truncateCommand = new NpgsqlCommand(truncateSql, destinationConnection, transaction);
                await truncateCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            await using var sourceCommand = new NpgsqlCommand(selectSql, sourceConnection);
            await using var sourceReader = await sourceCommand.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);

            await using var binaryImporter = destinationConnection.BeginBinaryImport(copySql);

            long totalCopiedRows = 0;
            long rowsSinceLastEvent = 0;
            var lastProgressPush = Stopwatch.StartNew();

            while (await sourceReader.ReadAsync(cancellationToken))
            {
                binaryImporter.StartRow();

                for (var index = 0; index < columns.Length; index++)
                {
                    if (await sourceReader.IsDBNullAsync(index, cancellationToken))
                    {
                        binaryImporter.WriteNull();
                        continue;
                    }

                    binaryImporter.Write(sourceReader.GetValue(index));
                }

                totalCopiedRows++;
                rowsSinceLastEvent++;

                if (rowsSinceLastEvent >= 1_000 || lastProgressPush.Elapsed >= TimeSpan.FromSeconds(1))
                {
                    var tableProgress = job.AddProcessedRows(tableRequest.Table, rowsSinceLastEvent);
                    await PublishProgressAsync(job, tableProgress, cancellationToken);

                    rowsSinceLastEvent = 0;
                    lastProgressPush.Restart();
                }
            }

            if (rowsSinceLastEvent > 0)
            {
                var tableProgress = job.AddProcessedRows(tableRequest.Table, rowsSinceLastEvent);
                await PublishProgressAsync(job, tableProgress, cancellationToken);
            }

            binaryImporter.Complete();
            await transaction.CommitAsync(cancellationToken);

            return totalCopiedRows;
        }
        catch
        {
            await transaction.RollbackAsync(cancellationToken);
            throw;
        }
    }

    private async Task PublishProgressAsync(
        MigrationJobRuntime job,
        TableProgressSnapshot? changedTable,
        CancellationToken cancellationToken)
    {
        var snapshot = job.GetSnapshot();
        var progressEvent = new MigrationProgressEvent(
            snapshot.JobId,
            snapshot.Status,
            changedTable,
            snapshot.CompletedTables,
            snapshot.TotalTables,
            snapshot.FailedTables,
            snapshot.ProcessedRows,
            snapshot.EstimatedRows,
            snapshot.ThroughputRowsPerSecond,
            DateTimeOffset.UtcNow);

        try
        {
            await _hubContext.Clients
                .Group(snapshot.JobId.ToString("N"))
                .SendAsync("migrationProgress", progressEvent, cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Falha ao publicar progresso para o job {JobId}", snapshot.JobId);
        }
    }

    private static bool IsTransient(Exception exception)
    {
        if (exception is TimeoutException)
        {
            return true;
        }

        if (exception is NpgsqlException npgsqlException && npgsqlException.IsTransient)
        {
            return true;
        }

        if (exception.InnerException is not null)
        {
            return IsTransient(exception.InnerException);
        }

        return false;
    }

    private sealed class MigrationJobRuntime
    {
        private readonly object _sync = new();
        private readonly DateTimeOffset _startedAt = DateTimeOffset.UtcNow;
        private readonly Dictionary<TableIdentifier, TableRuntimeState> _tables;
        private MigrationJobStatus _status = MigrationJobStatus.Pending;
        private DateTimeOffset? _finishedAt;
        private string? _errorMessage;

        public MigrationJobRuntime(MigrationStartRequest request)
        {
            JobId = Guid.NewGuid();
            _tables = request.Tables.ToDictionary(
                table => table.Table,
                table => new TableRuntimeState(
                    table.Table,
                    TableMigrationStatus.Pending,
                    0,
                    0,
                    table.EstimatedRows,
                    null,
                    DateTimeOffset.UtcNow,
                    null));
        }

        public Guid JobId { get; }

        public void SetJobStatus(MigrationJobStatus status, string? errorMessage)
        {
            lock (_sync)
            {
                _status = status;
                _errorMessage = errorMessage;
            }
        }

        public void MarkFinished()
        {
            lock (_sync)
            {
                _finishedAt = DateTimeOffset.UtcNow;
            }
        }

        public TableProgressSnapshot UpdateTable(
            TableIdentifier table,
            TableMigrationStatus status,
            int attempt,
            string? message)
        {
            lock (_sync)
            {
                var now = DateTimeOffset.UtcNow;
                var current = _tables[table];
                var updated = current with
                {
                    Status = status,
                    Attempt = attempt,
                    Message = message,
                    UpdatedAt = now,
                    LastAttemptStartedAt = status is TableMigrationStatus.Running or TableMigrationStatus.Retrying
                        ? now
                        : current.LastAttemptStartedAt
                };

                _tables[table] = updated;
                return updated.ToSnapshot();
            }
        }

        public TableProgressSnapshot AddProcessedRows(TableIdentifier table, long delta)
        {
            lock (_sync)
            {
                var now = DateTimeOffset.UtcNow;
                var current = _tables[table];
                var updated = current with
                {
                    ProcessedRows = current.ProcessedRows + delta,
                    UpdatedAt = now
                };

                _tables[table] = updated;
                return updated.ToSnapshot();
            }
        }

        public TableProgressSnapshot MarkTableCompleted(TableIdentifier table, int attempt, long totalRows, string? message)
        {
            lock (_sync)
            {
                var now = DateTimeOffset.UtcNow;
                var current = _tables[table];
                var updated = current with
                {
                    Status = TableMigrationStatus.Completed,
                    Attempt = attempt,
                    ProcessedRows = Math.Max(current.ProcessedRows, totalRows),
                    Message = message,
                    UpdatedAt = now
                };

                _tables[table] = updated;
                return updated.ToSnapshot();
            }
        }

        public TableProgressSnapshot MarkTableFailed(TableIdentifier table, int attempt, string? message)
        {
            lock (_sync)
            {
                var now = DateTimeOffset.UtcNow;
                var current = _tables[table];
                var updated = current with
                {
                    Status = TableMigrationStatus.Failed,
                    Attempt = attempt,
                    Message = message,
                    UpdatedAt = now
                };

                _tables[table] = updated;
                return updated.ToSnapshot();
            }
        }

        public MigrationJobSnapshot GetSnapshot()
        {
            lock (_sync)
            {
                var now = DateTimeOffset.UtcNow;
                var tableSnapshots = _tables.Values
                    .Select(table => table.ToSnapshot())
                    .OrderBy(table => table.Table.Schema, StringComparer.Ordinal)
                    .ThenBy(table => table.Table.Name, StringComparer.Ordinal)
                    .ToArray();

                var completed = tableSnapshots.Count(table =>
                    table.Status is TableMigrationStatus.Completed or TableMigrationStatus.Skipped);

                var failed = tableSnapshots.Count(table => table.Status == TableMigrationStatus.Failed);
                var processedRows = tableSnapshots.Sum(table => table.ProcessedRows);
                var estimatedRows = tableSnapshots
                    .Where(table => table.EstimatedRows.HasValue)
                    .Sum(table => table.EstimatedRows ?? 0);

                var elapsedSeconds = Math.Max((now - _startedAt).TotalSeconds, 1);
                var throughput = processedRows / elapsedSeconds;

                return new MigrationJobSnapshot(
                    JobId,
                    _status,
                    _startedAt,
                    _finishedAt,
                    tableSnapshots.Length,
                    completed,
                    failed,
                    processedRows,
                    estimatedRows,
                    throughput,
                    tableSnapshots,
                    _errorMessage);
            }
        }

        private sealed record TableRuntimeState(
            TableIdentifier Table,
            TableMigrationStatus Status,
            int Attempt,
            long ProcessedRows,
            long? EstimatedRows,
            string? Message,
            DateTimeOffset UpdatedAt,
            DateTimeOffset? LastAttemptStartedAt)
        {
            public TableProgressSnapshot ToSnapshot()
            {
                var effectiveUpdatedAt = UpdatedAt == default ? DateTimeOffset.UtcNow : UpdatedAt;
                var elapsedSeconds = LastAttemptStartedAt.HasValue
                    ? Math.Max((effectiveUpdatedAt - LastAttemptStartedAt.Value).TotalSeconds, 1)
                    : 1;

                var rowsPerSecond = ProcessedRows / elapsedSeconds;
                var progressPercent = EstimatedRows is > 0
                    ? Math.Min((ProcessedRows / (double)EstimatedRows.Value) * 100d, 100d)
                    : 0d;

                return new TableProgressSnapshot(
                    Table,
                    Status,
                    Attempt,
                    ProcessedRows,
                    EstimatedRows,
                    progressPercent,
                    rowsPerSecond,
                    Message,
                    effectiveUpdatedAt);
            }
        }
    }
}
