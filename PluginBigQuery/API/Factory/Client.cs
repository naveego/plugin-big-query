using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using PluginBigQuery.Helper;

namespace PluginBigQuery.API.Factory
{
    public class Client : IClient
    {

        private readonly BigQueryClient _client;
        private readonly string _projectId;
        private readonly string _defaultDatabase;
        
        public Client(Settings settings)
        {
            //Initialize client
            var credentials = GoogleCredential.FromFile(settings.JsonFilePath);
            _client = BigQueryClient.Create(settings.ProjectId, credentials);
            _projectId = settings.ProjectId;
            _defaultDatabase = settings.DefaultDatabase;
        }

        public async Task<BigQueryResults> ExecuteReaderAsync(string query)
        {
            return await _client.ExecuteQueryAsync(query, parameters: null);
        }

        public async Task<BigQueryResults> ExecuteReaderAsync(string query, IEnumerable<BigQueryParameter> parameters)
        {
            return await _client.ExecuteQueryAsync(query, parameters);
        }

        
        public async Task<bool> PingAsync()
        {
            await _client.ExecuteQueryAsync("SELECT 1;", parameters: null);
            return true;
        }

        public string GetProjectId()
        {
            return _projectId;
        }

        public string GetDefaultDatabase()
        {
            return _defaultDatabase;
        }
    }
}