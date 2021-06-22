using DbUp.Engine;

namespace DbUp.CassandraCql
{
    /// <summary>
    /// This preprocessor makes adjustments to your sql to make it compatible with Cassandra.
    /// </summary>
    public class CassandraPreprocessor : IScriptPreprocessor
    {
        /// <summary>
        /// Performs some preprocessing step on a Cassandra script.
        /// </summary>
        public string Process(string contents) => contents;
    }
}