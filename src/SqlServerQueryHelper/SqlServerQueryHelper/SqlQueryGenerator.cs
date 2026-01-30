using System.Text;

namespace SqlServerQueryHelper;

public class SqlQueryGenerator
{
    public static string CreateIndexIfNotExists(string tableName, string indexName, string script)
    {
        tableName = tableName.Trim();
        indexName = indexName.Trim().Trim('[', ']');

        string sql = $"""
            IF NOT EXISTS(SELECT * FROM sys.indexes WHERE object_id = object_id('{tableName}') AND NAME = '{indexName}')
            BEGIN
                {script};
            END;
            """;

        return sql;
    }

    public static string CreateIndexIfNotExists(string script)
    {
        var indexInfor = SqlQueryIndexParser.ParseIndex(script);

        if (string.IsNullOrWhiteSpace(indexInfor?.TableName) || string.IsNullOrWhiteSpace(indexInfor?.Name))
        {
            throw new ArgumentException($"The script must contain a valid table name and index name. TableName: {indexInfor?.TableName}, Name: {indexInfor?.Name}");
        }

        return CreateIndexIfNotExists(indexInfor.TableName, indexInfor.Name, script);
    }

    public static string DropIndexIfExists(string tableName, string indexName)
    {
        tableName = tableName.Trim();
        indexName = indexName.Trim().Trim('[', ']');

        string sql = $"""
            IF EXISTS(SELECT * FROM sys.indexes WHERE object_id = object_id('{tableName}') AND NAME = '{indexName}')
            BEGIN
                DROP INDEX [{indexName}] ON {tableName}
            END;
            """;

        return sql;
    }

    public static string DeleteDuplicatedRecords(string tableName, string[] duplicatedColumns, string[] orderByColumns, string id)
    {
        string autoColumn = Guid.NewGuid().ToString();

        var sqlBuilder = new StringBuilder();

        sqlBuilder.AppendLine("DELETE T");
        sqlBuilder.AppendLine("FROM");
        sqlBuilder.AppendLine("(");
        sqlBuilder.AppendLine($"SELECT [{id}], [{autoColumn}] = ROW_NUMBER() OVER (");
        sqlBuilder.AppendLine($"              PARTITION BY {string.Join(", ", duplicatedColumns.Select(x => $"[{x}]"))}");
        sqlBuilder.AppendLine($"              ORDER BY {string.Join(", ", orderByColumns.Select(x => $"[{x}]"))}");
        sqlBuilder.AppendLine("            )");
        sqlBuilder.AppendLine($"FROM {tableName}");
        sqlBuilder.AppendLine(") AS T");
        sqlBuilder.AppendLine($"WHERE [{autoColumn}] > 1");

        var sql = sqlBuilder.ToString();
        return sql;
    }

    public static string CountDuplicatedRecords(string tableName, string[] columns)
    {
        var sqlBuilder = new StringBuilder();

        sqlBuilder.AppendLine($"SELECT {string.Join(", ", columns.Select(x => $"[{x}]"))}, COUNT(*) AS COUNT");
        sqlBuilder.AppendLine($"FROM {tableName}");
        sqlBuilder.AppendLine($"GROUP BY {string.Join(", ", columns.Select(x => $"[{x}]"))}");
        sqlBuilder.AppendLine($"HAVING COUNT(*) > 1");

        var sql = sqlBuilder.ToString();
        return sql;
    }

    public static string ExecuteWithLock(string sql, string lockName)
    {
        string wrapper = $"""
            DECLARE @Resource nvarchar(255) = '{lockName}';
            BEGIN TRY
                EXEC sp_getapplock @Resource = @Resource, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = -1, @DbPrincipal = 'public';
                {sql}
                EXEC sp_releaseapplock @Resource, 'Session';
            END TRY
            BEGIN CATCH
                IF APPLOCK_MODE('public', @Resource, 'Session') != 'NoLock'
                    EXEC dbo.sp_releaseapplock @Resource, 'Session';
                THROW
            END CATCH
            """;
        return wrapper;
    }
}
