using System;
using System.Data;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using DbUp.Support;

namespace DbUp.CassandraCql
{
    /// <summary>
    /// An implementation of the <see cref="IJournal"/> interface which tracks version numbers for a 
    /// Cassandra database using a table called SchemaVersions.
    /// </summary>
    public class CassandraTableJournal : TableJournal
    {
        bool journalExists;
        /// <summary>
        /// Creates a new Cassandra table journal.
        /// </summary>
        /// <param name="connectionManager">The Cassandra connection manager.</param>
        /// <param name="logger">The upgrade logger.</param>
        /// <param name="schema">The name of the schema the journal is stored in.</param>
        /// <param name="tableName">The name of the journal table.</param>
        public CassandraTableJournal(Func<IConnectionManager> connectionManager, Func<IUpgradeLog> logger, string schema, string tableName)
            : base(connectionManager, logger, new CassandraObjectParser(), schema, tableName)
        {
        }

        protected override string CreateSchemaTableSql(string quotedPrimaryKeyName)
        {
            var fqSchemaTableName = UnquotedSchemaTableName;
            return
                 $@"CREATE TABLE {fqSchemaTableName}
                (
                    schemaversionsid uuid,
                    scriptname text,
                    applied timestamp,
                    PRIMARY KEY (schemaversionsid, scriptname)
                )";
        }

        protected override string GetInsertJournalEntrySql(string scriptName, string applied)
        {
            return $"insert into {FqSchemaTableName} (schemaversionsid, scriptname, applied) values ({Guid.NewGuid()}, \'{scriptName}\', \'{applied}\')";
        }

        protected override string DoesTableExistSql()
        {
            var builder = UnquotedSchemaTableName.Split(".");

            var keySpaceName = builder.Length > 1 ? builder[0] : "";
            var tableName = builder.Length > 1 ? builder[1] : UnquotedSchemaTableName;

            var keySpaceFilter = !string.IsNullOrEmpty(keySpaceName) ? $" AND keyspace_name = '{keySpaceName}'" : "";

            return $"SELECT count(*) FROM system_schema.tables WHERE table_name = '{tableName}' {keySpaceFilter} ALLOW FILTERING";
        }

        protected override string GetJournalEntriesSql()
        {
            return $"select scriptname from {FqSchemaTableName}";
        }

        /// <summary>
        /// Records a database upgrade for a Cassandra database in a given connection string.
        /// </summary>
        /// <param name="script">The script.</param>
        /// <param name="dbCommandFactory"></param>
        public override void StoreExecutedScript(SqlScript script, Func<IDbCommand> dbCommandFactory)
        {
            EnsureTableExistsAndIsLatestVersion(dbCommandFactory);
            using (var command = dbCommandFactory())
            {
                command.CommandType = CommandType.Text;
                command.CommandText = GetInsertJournalEntrySql(script.Name, DateTime.UtcNow.ToString("s"));
                command.ExecuteNonQuery();
            }
        }
    }
}
