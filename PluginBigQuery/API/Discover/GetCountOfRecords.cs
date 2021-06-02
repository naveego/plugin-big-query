using System;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginBigQuery.API.Factory;

namespace PluginBigQuery.API.Discover
{
    public static partial class Discover
    {
        //public static async Task<Count> GetCountOfRecords(IConnectionFactory connFactory, Schema schema)
        public static async Task<Count> GetCountOfRecords(IClientFactory clientFactory, Schema schema)
        {
            var query = schema.Query;
            
            var client = clientFactory.GetClient();
            
            if (string.IsNullOrWhiteSpace(query))
            {
                query = $"SELECT * FROM {schema.Id}";
            }

            try
            {
                var result = await client.ExecuteReaderAsync($"SELECT COUNT(*) as count FROM ({query}) as q");

                var count = 0;

                foreach (var row in result)
                {
                    count = Convert.ToInt32(row["count"]);
                }

                return count == 0
                    ? new Count
                    {
                        Kind = Count.Types.Kind.Unavailable,
                    }
                    : new Count
                    {
                        Kind = Count.Types.Kind.Exact,
                        Value = count
                    };
            }
            catch (Exception e)
            {
                var noop = e.Message;
                throw;
            }
            finally
            {
                //await conn.CloseAsync();
            }
        }
    }
}