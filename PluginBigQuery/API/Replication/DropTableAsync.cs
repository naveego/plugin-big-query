using System;
using System.Threading.Tasks;
using MySqlConnector.Logging;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {
        private static readonly string DropTableQuery = @"DROP TABLE IF EXISTS {0}.{1}";

        public static async Task DropTableAsync(IClientFactory clientFactory, ReplicationTable table)
        {
            var client = clientFactory.GetClient();

            try
            {
                string query = string.Format(DropTableQuery,
                    Utility.Utility.GetSafeName(table.SchemaName, '`'),
                    Utility.Utility.GetSafeName(table.TableName, '`')
                );
                client.ExecuteReaderAsync(query);
            }
            finally
            {
                //noop
            }
        }
    }
}