using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using PluginBigQuery.Helper;

namespace PluginBigQuery.API.Factory
{
    public class ClientFactory : IClientFactory
    {
        private Settings _settings;
        
        public void Initialize(Settings settings)
        {
            _settings = settings;
        }

        public IClient GetClient()
        {
            return new Client(_settings);
        }
    }
}