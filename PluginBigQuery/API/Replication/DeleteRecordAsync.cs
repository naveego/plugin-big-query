using System.Threading.Tasks;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {
        private static readonly string DeleteRecordQuery = @"DELETE FROM {0}.{1}
WHERE {2} = '{3}'";

        public static async Task DeleteRecordAsync(IClientFactory clientFactory, ReplicationTable table,
            string primaryKeyValue)
        {
            var client = clientFactory.GetClient();

            var query = string.Format(DeleteRecordQuery,
                Utility.Utility.GetSafeName(table.SchemaName, '`'),
                Utility.Utility.GetSafeName(table.TableName, '`'),
                Utility.Utility.GetSafeName(table.Columns.Find(c => c.PrimaryKey == true).ColumnName, '`', true),
                primaryKeyValue
            );
            
            try
            {
                await client.ExecuteReaderAsync(query);
            }
            finally
            {
                //noop
            }
        }
    }
}