using Npgsql;

namespace SchemaFlow.Api.Infrastructure;

public static class ConnectionStringMasking
{
    public static string Mask(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return "<empty>";
        }

        try
        {
            var builder = new NpgsqlConnectionStringBuilder(connectionString);

            if (!string.IsNullOrWhiteSpace(builder.Username))
            {
                builder.Username = "***";
            }

            if (!string.IsNullOrWhiteSpace(builder.Password))
            {
                builder.Password = "***";
            }

            if (!string.IsNullOrWhiteSpace(builder.Passfile))
            {
                builder.Passfile = "***";
            }

            return builder.ToString();
        }
        catch
        {
            return "<invalid-connection-string>";
        }
    }
}
