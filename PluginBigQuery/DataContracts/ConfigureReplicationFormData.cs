namespace PluginBigQuery.DataContracts
{
    public class ConfigureReplicationFormData
    {
        public string GoldenTableName { get; set; }
        public string VersionTableName { get; set; }
    }
}