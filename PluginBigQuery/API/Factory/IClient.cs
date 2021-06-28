using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Cloud.BigQuery.V2;

namespace PluginBigQuery.API.Factory
{
    public interface IClient
    {
        Task<BigQueryResults> ExecuteReaderAsync(string query);
        string GetDefaultDatabase();

        Task<bool> PingAsync();
    }
}