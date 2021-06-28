using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {
        private static readonly string GetRecordQuery = @"SELECT * FROM {0}.{1}
WHERE {2} = '{3}'";

        public static async Task<Dictionary<string, object>> GetRecordAsync(IClientFactory clientFactory,
            ReplicationTable table,
            string primaryKeyValue)
        {
            var client = clientFactory.GetClient();
            
            try
            {
                var query = string.Format(GetRecordQuery,
                    Utility.Utility.GetSafeName(table.SchemaName, '`', true),
                    Utility.Utility.GetSafeName(table.TableName, '`', true),
                    Utility.Utility.GetSafeName(table.Columns.Find(c => c.PrimaryKey == true).ColumnName, '`', true),
                    primaryKeyValue
                );

                var results = await client.ExecuteReaderAsync(query);
                
                Dictionary<string, object> recordMap = null;
                // check if record exists

                foreach (var row in results)
                {
                    recordMap = new Dictionary<string, object>();

                    foreach (var field in results.Schema.Fields)
                    {
                        try
                        {
                            recordMap[field.Name] = row[field.Name];
                        }
                        catch (Exception e)
                        {
                            Logger.Error(e, $"No column with column name: {field.Name}");
                            Logger.Error(e, e.Message);
                            recordMap[field.Name] = null;
                        }
                    }
                }

                return recordMap;
            }
            finally
            {
                //noop
            }
        }
    }
}