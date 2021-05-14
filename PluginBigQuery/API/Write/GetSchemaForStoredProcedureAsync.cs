using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Write
{
    public static partial class Write
    {
        private static string ParamName = "PARAMETER_NAME";
        private static string DataType = "DATA_TYPE";

        private static string GetStoredProcedureParamsQuery = @"
SELECT PARAMETER_NAME, DATA_TYPE, ORDINAL_POSITION
FROM {0}.INFORMATION_SCHEMA.PARAMETERS
WHERE SPECIFIC_SCHEMA = '{1}'
AND SPECIFIC_NAME = '{2}'
ORDER BY ORDINAL_POSITION ASC";

        public static async Task<Schema> GetSchemaForStoredProcedureAsync(IClientFactory clientFactory,
            WriteStoredProcedure storedProcedure)
        {
            var schema = new Schema
            {
                Id = storedProcedure.GetId(),
                Name = storedProcedure.GetId(),
                Description = "",
                DataFlowDirection = Schema.Types.DataFlowDirection.Write,
                Query = storedProcedure.GetId()
            };

            var client = clientFactory.GetClient();

            var query = string.Format(GetStoredProcedureParamsQuery, "testdata", storedProcedure.SchemaName,
                storedProcedure.SpecificName);
            var results = await client.ExecuteReaderAsync(query);

            foreach (var row in results)
            {
                var property = new Property()
                {
                    Id = row[ParamName].ToString(),
                    Name = row[ParamName].ToString(),
                    Description = "",
                    Type = Discover.Discover.GetType(row[DataType].ToString()),
                    TypeAtSource = row[DataType].ToString()
                };
                
                schema.Properties.Add(property);
            }

            return schema;
        }
    }
}