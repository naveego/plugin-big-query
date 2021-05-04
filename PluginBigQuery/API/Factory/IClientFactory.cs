using PluginBigQuery.Helper;

namespace PluginBigQuery.API.Factory
{
    public interface IClientFactory
    {
        void Initialize(Settings settings);
        IClient GetClient();
    }
}