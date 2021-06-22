using System;
using System.Data;
using System.Security.Cryptography.X509Certificates;
using DbUp.Builder;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using Cassandra;
using Cassandra.Data;

namespace DbUp.CassandraCql
{

    // ReSharper disable once CheckNamespace

    /// <summary>
    /// Configuration extension methods for Cassandra.
    /// </summary>
    public static class CassandraExtensions
    {

        /// <summary>
        /// Creates an upgrader for Cassandra databases.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionString">Cassandra database connection string.</param>
        /// <returns>
        /// A builder for a database upgrader designed for Cassandra databases.
        /// </returns>
        public static UpgradeEngineBuilder CassandraDatabase(this SupportedDatabases supported, string connectionString)
            => CassandraDatabase(supported, connectionString, null);

        /// <summary>
        /// Creates an upgrader for Cassandra databases.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionString">Cassandra database connection string.</param>
        /// <param name="schema">The schema in which to check for changes</param>
        /// <returns>
        /// A builder for a database upgrader designed for Cassandra databases.
        /// </returns>
        public static UpgradeEngineBuilder CassandraDatabase(this SupportedDatabases supported, string connectionString, string schema)
            => CassandraDatabase(new CassandraConnectionManager(connectionString), schema);

        /// <summary>
        /// Creates an upgrader for Cassandra databases.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionManager">The <see cref="CassandraConnectionManager"/> to be used during a database upgrade.</param>
        /// <returns>
        /// A builder for a database upgrader designed for Cassandra databases.
        /// </returns>
        public static UpgradeEngineBuilder CassandraDatabase(this SupportedDatabases supported, IConnectionManager connectionManager)
            => CassandraDatabase(connectionManager);

        /// <summary>
        /// Creates an upgrader for Cassandra databases.
        /// </summary>
        /// <param name="connectionManager">The <see cref="CassandraConnectionManager"/> to be used during a database upgrade.</param>
        /// <returns>
        /// A builder for a database upgrader designed for Cassandra databases.
        /// </returns>
        public static UpgradeEngineBuilder CassandraDatabase(IConnectionManager connectionManager)
            => CassandraDatabase(connectionManager, null);


        /// <summary>
        /// Ensures that the database specified in the connection string exists.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <returns></returns>
        public static void CassandraDatabase(this SupportedDatabasesForEnsureDatabase supported, string connectionString)
        {
            CassandraDatabase(supported, connectionString, new ConsoleUpgradeLog());
        }

        /// <summary>
        /// Ensures that the database specified in the connection string exists using SSL for the connection.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="certificate">Certificate for securing connection.</param>
        /// <returns></returns>
        public static void CassandraDatabase(this SupportedDatabasesForEnsureDatabase supported, string connectionString, X509Certificate2 certificate)
        {
            CassandraDatabase(supported, connectionString, new ConsoleUpgradeLog(), certificate);
        }

        /// <summary>
        /// Ensures that the database specified in the connection string exists.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="logger">The <see cref="DbUp.Engine.Output.IUpgradeLog"/> used to record actions.</param>
        /// <returns></returns>
        public static void CassandraDatabase(this SupportedDatabasesForEnsureDatabase supported, string connectionString, IUpgradeLog logger)
        {
            CassandraDatabase(supported, connectionString, logger, null);
        }

        private static void CassandraDatabase(this SupportedDatabasesForEnsureDatabase supported, string connectionString, IUpgradeLog logger, X509Certificate2 certificate)
        {
            if (supported == null) throw new ArgumentNullException("supported");

            if (string.IsNullOrEmpty(connectionString) || connectionString.Trim() == string.Empty)
            {
                throw new ArgumentNullException("connectionString");
            }

            if (logger == null) throw new ArgumentNullException("logger");

            var masterConnectionStringBuilder = new CassandraConnectionStringBuilder(connectionString);

            var keySpaceName = masterConnectionStringBuilder.DefaultKeyspace;

            if (string.IsNullOrEmpty(keySpaceName) || keySpaceName.Trim() == string.Empty)
            {
                throw new InvalidOperationException("The connection string does not specify a database name.");
            }

            masterConnectionStringBuilder.DefaultKeyspace = "cassandra";

            var logMasterConnectionStringBuilder = new CassandraConnectionStringBuilder(masterConnectionStringBuilder.ConnectionString);
            if (!string.IsNullOrEmpty(logMasterConnectionStringBuilder.Password))
            {
                logMasterConnectionStringBuilder.Password = string.Empty.PadRight(masterConnectionStringBuilder.Password.Length, '*');
            }

            logger.WriteInformation("Master ConnectionString => {0}", logMasterConnectionStringBuilder.ConnectionString);

            using (var connection = new CqlConnection(masterConnectionStringBuilder.ConnectionString))
            {
                // check to see if the keyspace already exists..

                connection.Open();

                var sqlCommandText = string.Format
                    (
                        "create keyspace if not exists \"{0}\" WITH replication = {'class':'SimpleStrategy', 'replication_factor' : 1};", // replication_factor should be configurable
                        keySpaceName
                    );

                // Create the database...
                using (var command = new CqlCommand()
                {
                    CommandType = CommandType.Text,
                    CommandText = sqlCommandText,
                    Connection = connection
                })
                {
                    command.ExecuteNonQuery();
                }

                logger.WriteInformation(@"Created keyspace {0}", keySpaceName);
            }
        }

        /// <summary>
        /// Tracks the list of executed scripts in a SQL Server table.
        /// </summary>
        /// <param name="builder">The builder.</param>
        /// <param name="schema">The schema.</param>
        /// <param name="table">The table.</param>
        /// <returns></returns>
        public static UpgradeEngineBuilder JournalToCassandraTable(this UpgradeEngineBuilder builder, string schema, string table)
        {
            builder.Configure(c => c.Journal = new CassandraTableJournal(() => c.ConnectionManager, () => c.Log, schema, table));
            return builder;
        }

        /// <summary>
        /// Creates an upgrader for Cassandra databases.
        /// </summary>
        /// <param name="connectionManager">The <see cref="CassandraConnectionManager"/> to be used during a database upgrade.</param>
        /// /// <param name="schema">Which Cassandra schema to check for changes</param>
        /// <returns>
        /// A builder for a database upgrader designed for Cassandra databases.
        /// </returns>
        public static UpgradeEngineBuilder CassandraDatabase(IConnectionManager connectionManager, string schema)
        {
            var builder = new UpgradeEngineBuilder();
            builder.Configure(c => c.ConnectionManager = connectionManager);
            builder.Configure(c => c.ScriptExecutor = new CassandraScriptExecutor(() => c.ConnectionManager, () => c.Log, null, () => c.VariablesEnabled, c.ScriptPreprocessors, () => c.Journal));
            builder.Configure(c => c.Journal = new CassandraTableJournal(() => c.ConnectionManager, () => c.Log, null, (string.IsNullOrWhiteSpace(schema) ? "schemaversions" : $"{schema}.schemaversions")));
            builder.WithPreprocessor(new CassandraPreprocessor());
            return builder;
        }
    }
}
