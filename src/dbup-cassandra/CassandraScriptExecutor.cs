using System;
using System.Collections.Generic;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using DbUp.Support;
using Cassandra;

namespace DbUp.CassandraCql
{
    /// <summary>
    /// An implementation of <see cref="ScriptExecutor"/> that executes against a Cassandra database.
    /// </summary>
    public class CassandraScriptExecutor : ScriptExecutor
    {
        /// <summary>
        /// Initializes an instance of the <see cref="CassandraScriptExecutor"/> class.
        /// </summary>
        /// <param name="connectionManagerFactory"></param>
        /// <param name="log">The logging mechanism.</param>
        /// <param name="schema">The schema that contains the table.</param>
        /// <param name="variablesEnabled">Function that returns <c>true</c> if variables should be replaced, <c>false</c> otherwise.</param>
        /// <param name="scriptPreprocessors">Script Preprocessors in addition to variable substitution</param>
        /// <param name="journalFactory">Database journal</param>
        public CassandraScriptExecutor(Func<IConnectionManager> connectionManagerFactory, Func<IUpgradeLog> log, string schema, Func<bool> variablesEnabled,
            IEnumerable<IScriptPreprocessor> scriptPreprocessors, Func<IJournal> journalFactory)
            : base(connectionManagerFactory, new CassandraObjectParser(), log, schema, variablesEnabled, scriptPreprocessors, journalFactory)
        {
        }

        protected override string GetVerifySchemaSql(string schema)
        {
            throw new NotSupportedException();
        }

        protected override void ExecuteCommandsWithinExceptionHandler(int index, SqlScript script, Action executeCommand)
        {
            try
            {
                executeCommand();
            }
            catch (DriverException exception)
            {
                Log().WriteInformation("Cassandra exception has occured in script: '{0}'", script.Name);
                Log().WriteError("Script block number: {0}; Message: {1}", index, exception.Message);
                Log().WriteError(exception.ToString());
                throw;
            }
        }

    }

}
