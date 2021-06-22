using Cassandra.Data;
using DbUp.Engine.Transactions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace DbUp.CassandraCql
{
    /// <summary>
    /// Manages Cassandra database connections.
    /// </summary>
    public class CassandraConnectionManager : DatabaseConnectionManager
    {
        /// <summary>
        /// Creates a new Cassandra database connection.
        /// </summary>
        /// <param name="connectionString">The Cassandra connection string.</param>
        public CassandraConnectionManager(string connectionString)
            : base(new DelegateConnectionFactory(l => new CqlConnection(connectionString)))
        {
        }

        /// <summary>
        /// Splits the statements in the script using the ";" character.
        /// </summary>
        /// <param name="scriptContents">The contents of the script to split.</param>
        public override IEnumerable<string> SplitScriptIntoCommands(string scriptContents)
        {
            //var scriptStatements =
            //    Regex.Split(scriptContents, "^\\s*;\\s*$", RegexOptions.IgnoreCase | RegexOptions.Multiline)
            //        .Select(x => x.Trim())
            //        .Where(x => x.Length > 0)
            //        .ToArray();

            var stringSeparators = new string[] { ";" };
            var parts = scriptContents.Split(stringSeparators, StringSplitOptions.RemoveEmptyEntries).ToList();
            var scriptStatements = parts.Select(x => x.Trim()).Where(x => x.Length > 0).ToArray();

            return scriptStatements;
        }
    }
}
