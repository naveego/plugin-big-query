using System.Collections;
using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Cloud.BigQuery.V2;

namespace PluginBigQuery.API.Factory
{
    public interface IClient
    {
        Task<BigQueryResults> ExecuteReaderAsync(string query);
        Task<BigQueryResults> ExecuteReaderAsync(string query, IEnumerable<BigQueryParameter> parameters);

        string GetProjectId(); // Make unit-test for this
        string GetDefaultDatabase();

        Task<bool> PingAsync();
    }
}