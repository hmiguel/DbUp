using DbUp.Support;

namespace DbUp.CassandraCql
{
    /// <summary>
    /// Parses Sql Objects and performs quoting functions.
    /// </summary>
    public class CassandraObjectParser : SqlObjectParser
    {
        public CassandraObjectParser() : base("", "")
        {
        }
    }
}
