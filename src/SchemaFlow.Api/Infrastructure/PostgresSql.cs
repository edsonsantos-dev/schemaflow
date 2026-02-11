namespace SchemaFlow.Api.Infrastructure;

public static class PostgresSql
{
    public static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new ArgumentException("Identifier cannot be null or empty.", nameof(identifier));
        }

        return $"\"{identifier.Replace("\"", "\"\"")}\"";
    }

    public static string QualifiedTable(string schema, string table)
        => $"{QuoteIdentifier(schema)}.{QuoteIdentifier(table)}";
}
