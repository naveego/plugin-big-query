using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginBigQuery.API.Factory;

namespace PluginBigQuery.API.Discover
{
    public static partial class Discover
    {
        private const string GetTableAndColumnsQuery = @"
SELECT t.TABLE_NAME
     , t.TABLE_SCHEMA
     , t.TABLE_TYPE
     , c.COLUMN_NAME
     , c.DATA_TYPE
     , 0 as COLUMN_KEY
     , c.IS_NULLABLE
     , 0 as CHARACTER_MAXIMUM_LENGTH

FROM {0}.INFORMATION_SCHEMA.TABLES AS t
      INNER JOIN {0}.INFORMATION_SCHEMA.COLUMNS AS c ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME

AND t.TABLE_NAME = '{1}' 
AND t.TABLE_SCHEMA = '{2}'

ORDER BY t.TABLE_NAME";
        

        public static async Task<Schema> GetRefreshSchemaForTable(IClientFactory clientFactory, Schema schema,
            int sampleSize = 5)
        {
            
            var decomposed = DecomposeSafeName(schema.Id).TrimEscape();
            var client = clientFactory.GetClient();
            string db = client.GetDefaultDatabase();

            string query = String.Format(GetTableAndColumnsQuery, db, decomposed.Table, decomposed.Schema);
            
            var results = await client.ExecuteReaderAsync(query);

            var refreshProperties = new List<Property>();

            
            
            foreach (var row in results)
            {
                foreach (var field in row.Schema.Fields)
                {
                    
                    var property = new Property(){};
                    switch (field.Name)
                    {
                        case "COLUMN_NAME":
                            property.Name = field.Name;
                            property.Id = field.Name;
                            break;
                        case "DATA_TYPE":
                            property.Type = GetType(row[field.Name].ToString());
                            property.TypeAtSource = row[field.Name].ToString(); //Max length does not exist, so just use dataType
                            break;
                        case "COLUMN_KEY":
                            property.IsKey = false;
                            break;
                        case "IS_NULLABLE":
                            property.IsNullable = true;
                            break;
                    }
                    refreshProperties.Add(property);
                }
            }
            
            schema.Properties.Clear();
            schema.Properties.AddRange(refreshProperties);

            return await AddSampleAndCount(clientFactory, schema, sampleSize);
            
        }

        private static DecomposeResponse DecomposeSafeName(string schemaId)
        {
            var response = new DecomposeResponse
            {
                Database = "",
                Schema = "",
                Table = ""
            };
            var parts = schemaId.Split('.');

            switch (parts.Length)
            {
                case 0:
                    return response;
                case 1:
                    response.Table = parts[0];
                    return response;
                case 2:
                    response.Schema = parts[0];
                    response.Table = parts[1];
                    return response;
                case 3:
                    response.Database = parts[0];
                    response.Schema = parts[1];
                    response.Table = parts[2];
                    return response;
                default:
                    return response;
            }
        }

        private static DecomposeResponse TrimEscape(this DecomposeResponse response, char escape = '`')
        {
            response.Database = response.Database.Trim(escape);
            response.Schema = response.Schema.Trim(escape);
            response.Table = response.Table.Trim(escape);

            return response;
        }
    }

    class DecomposeResponse
    {
        public string Database { get; set; }
        public string Schema { get; set; }
        public string Table { get; set; }
    }
}