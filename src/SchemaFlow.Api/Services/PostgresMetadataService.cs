using Npgsql;
using SchemaFlow.Api.Infrastructure;
using SchemaFlow.Shared.Contracts;

namespace SchemaFlow.Api.Services;

public sealed class PostgresMetadataService
{
    private readonly ILogger<PostgresMetadataService> _logger;

    public PostgresMetadataService(ILogger<PostgresMetadataService> logger)
    {
        _logger = logger;
    }

    public async Task<ConnectionTestResponse> TestConnectionAsync(string connectionString, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = new NpgsqlCommand("SELECT current_database(), version();", connection);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);

            string? database = null;
            string? version = null;

            if (await reader.ReadAsync(cancellationToken))
            {
                database = reader.GetString(0);
                version = reader.GetString(1);
            }

            return new ConnectionTestResponse(true, "Conexao estabelecida com sucesso.", database, version);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Falha ao testar conexao: {Connection}", ConnectionStringMasking.Mask(connectionString));
            return new ConnectionTestResponse(false, ex.Message);
        }
    }

    public async Task<IReadOnlyList<string>> GetSchemasAsync(string connectionString, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT nspname
            FROM pg_namespace
            WHERE nspname NOT LIKE 'pg_%'
              AND nspname <> 'information_schema'
            ORDER BY nspname;
            """;

        var schemas = new List<string>();

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            schemas.Add(reader.GetString(0));
        }

        return schemas;
    }

    public async Task<IReadOnlyList<TableMetadata>> GetTablesAsync(
        string connectionString,
        IReadOnlyCollection<string> schemas,
        CancellationToken cancellationToken)
    {
        if (schemas.Count == 0)
        {
            return [];
        }

        const string sql = """
            SELECT n.nspname AS schema_name,
                   c.relname AS table_name,
                   CASE WHEN c.reltuples < 0 THEN NULL ELSE GREATEST(c.reltuples::bigint, 0) END AS estimated_rows
            FROM pg_class c
            INNER JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p')
              AND n.nspname = ANY(@schemas)
            ORDER BY n.nspname, c.relname;
            """;

        var tables = new List<TableMetadata>();

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("schemas", schemas.ToArray());

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        while (await reader.ReadAsync(cancellationToken))
        {
            var identifier = new TableIdentifier(reader.GetString(0), reader.GetString(1));
            long? estimated = reader.IsDBNull(2) ? null : reader.GetInt64(2);
            tables.Add(new TableMetadata(identifier, estimated));
        }

        return tables;
    }

    public async Task<IReadOnlyDictionary<TableIdentifier, IReadOnlyList<ColumnMetadata>>> GetColumnsAsync(
        string connectionString,
        IReadOnlyCollection<TableIdentifier> tables,
        CancellationToken cancellationToken)
    {
        if (tables.Count == 0)
        {
            return new Dictionary<TableIdentifier, IReadOnlyList<ColumnMetadata>>();
        }

        var selectedTables = tables.ToHashSet();
        var schemas = selectedTables.Select(t => t.Schema).Distinct(StringComparer.Ordinal).ToArray();

        const string sql = """
            SELECT n.nspname AS schema_name,
                   c.relname AS table_name,
                   a.attname AS column_name,
                   a.attnum AS ordinal_position,
                   pg_catalog.format_type(a.atttypid, a.atttypmod) AS formatted_type,
                   a.attnotnull AS is_not_null,
                   pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) AS default_expr,
                   (a.attgenerated <> '') AS is_generated,
                   (a.attidentity <> '') AS is_identity
            FROM pg_attribute a
            INNER JOIN pg_class c ON c.oid = a.attrelid
            INNER JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
            WHERE c.relkind IN ('r', 'p')
              AND a.attnum > 0
              AND NOT a.attisdropped
              AND n.nspname = ANY(@schemas)
            ORDER BY n.nspname, c.relname, a.attnum;
            """;

        var result = new Dictionary<TableIdentifier, List<ColumnMetadata>>();

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("schemas", schemas);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var table = new TableIdentifier(reader.GetString(0), reader.GetString(1));
            if (!selectedTables.Contains(table))
            {
                continue;
            }

            if (!result.TryGetValue(table, out var columns))
            {
                columns = [];
                result[table] = columns;
            }

            columns.Add(new ColumnMetadata(
                reader.GetString(2),
                reader.GetString(4),
                !reader.GetBoolean(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.GetInt16(3),
                reader.GetBoolean(7),
                reader.GetBoolean(8)));
        }

        return result.ToDictionary(kvp => kvp.Key, kvp => (IReadOnlyList<ColumnMetadata>)kvp.Value);
    }

    public async Task<IReadOnlySet<string>> GetExistingSchemasAsync(
        string connectionString,
        IReadOnlyCollection<string> schemaNames,
        CancellationToken cancellationToken)
    {
        if (schemaNames.Count == 0)
        {
            return new HashSet<string>(StringComparer.Ordinal);
        }

        const string sql = """
            SELECT nspname
            FROM pg_namespace
            WHERE nspname = ANY(@schemas);
            """;

        var result = new HashSet<string>(StringComparer.Ordinal);

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("schemas", schemaNames.ToArray());

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(reader.GetString(0));
        }

        return result;
    }

    public async Task<IReadOnlySet<TableIdentifier>> GetExistingTablesAsync(
        string connectionString,
        IReadOnlyCollection<TableIdentifier> tables,
        CancellationToken cancellationToken)
    {
        if (tables.Count == 0)
        {
            return new HashSet<TableIdentifier>();
        }

        var selectedTables = tables.ToHashSet();
        var schemas = selectedTables.Select(t => t.Schema).Distinct(StringComparer.Ordinal).ToArray();

        const string sql = """
            SELECT n.nspname AS schema_name,
                   c.relname AS table_name
            FROM pg_class c
            INNER JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p')
              AND n.nspname = ANY(@schemas)
            ORDER BY n.nspname, c.relname;
            """;

        var result = new HashSet<TableIdentifier>();

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("schemas", schemas);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var table = new TableIdentifier(reader.GetString(0), reader.GetString(1));
            if (selectedTables.Contains(table))
            {
                result.Add(table);
            }
        }

        return result;
    }

    public async Task<bool> SchemaExistsAsync(string connectionString, string schemaName, CancellationToken cancellationToken)
    {
        const string sql = "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = @schema);";

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("schema", schemaName);

        return (bool)(await command.ExecuteScalarAsync(cancellationToken) ?? false);
    }
}
