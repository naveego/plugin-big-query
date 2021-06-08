using System.Threading.Tasks;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {
        private static readonly string RecordExistsQuery = @"SELECT COUNT(*) as c
FROM (
SELECT * FROM {0}.{1}
WHERE {2} = '{3}'    
) as q";

        public static async Task<bool> RecordExistsAsync(IClientFactory clientFactory, ReplicationTable table,
            string primaryKeyValue)
        {
            var client = clientFactory.GetClient();

            try
            {
                var query = string.Format(RecordExistsQuery,
                    Utility.Utility.GetSafeName(table.SchemaName, '`', true),
                    Utility.Utility.GetSafeName(table.TableName, '`', true),
                    Utility.Utility.GetSafeName(table.Columns.Find(c => c.PrimaryKey == true).ColumnName, '`', true),
                    primaryKeyValue
                );

                // check if record exists
                var results = await client.ExecuteReaderAsync(query);
                
                var count = (long) 0;
                foreach (var row in results)
                {
                    count = (long) row["c"];
                }

                return count != 0;
            }
            finally
            {
                //noop
            }

        }
    }
}
