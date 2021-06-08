using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {
        private static readonly string EnsureTableQuery = @"SELECT COUNT(*) as c
FROM {0}.INFORMATION_SCHEMA.TABLES 
WHERE table_schema = '{1}' 
AND table_name = '{2}'";

        public static async Task EnsureTableAsync(IClientFactory clientFactory, ReplicationTable table)
        {
            var client = clientFactory.GetClient();
            var db = client.GetDefaultDatabase();
            try
            {
                Logger.Info($"Creating Table... {table.SchemaName}.{table.TableName}");
                string bq_query = string.Format(EnsureTableQuery, db, 
                    Utility.Utility.GetSafeString(table.SchemaName, "\'", "\\\'"), 
                    Utility.Utility.GetSafeString(table.TableName, "\'", "\\\'"));

                await client.ExecuteReaderAsync(bq_query);
                var results = await client.ExecuteReaderAsync(bq_query);

                foreach (var row in results)
                {
                    var bq_count = (long) row["c"];

                    if (bq_count == 0)
                    {
                        // create table
                        var querySb = new StringBuilder($@"CREATE TABLE IF NOT EXISTS 
{Utility.Utility.GetSafeName(table.SchemaName, '`', true)}.{Utility.Utility.GetSafeName(table.TableName, '`', true)}(");
                        
                        foreach (var column in table.Columns)
                        {
                            querySb.Append(
                                $"{Utility.Utility.GetSafeName(column.ColumnName, '`', true)} {column.DataType},");
                            
                        }
                        querySb.Length--;
                        querySb.Append(");");
                        
                        var query = querySb.ToString();
                        Logger.Info($"Creating Table: {query}");
                        await client.ExecuteReaderAsync(query);
                    }
                }
            }
            catch (Exception e)
            {
                throw;
            }
            finally
            {
                
            }
        }
    }
}