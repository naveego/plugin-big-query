using System;
using System.Collections.Generic;
using System.Data;
using Google.Protobuf.Collections;
using Naveego.Sdk.Plugins;
using PluginBigQuery.API.Factory;

namespace PluginBigQuery.API.Discover
{
    public static partial class Discover
    {
        public static async IAsyncEnumerable<Schema> GetRefreshSchemas(IClientFactory clientFactory,
            RepeatedField<Schema> refreshSchemas, int sampleSize = 5)
        {
            try
            {

                foreach (var schema in refreshSchemas)
                {
                    if (string.IsNullOrWhiteSpace(schema.Query))
                    {
                        yield return await GetRefreshSchemaForTable(clientFactory, schema, sampleSize);
                        continue;
                    }
                    var client = clientFactory.GetClient();

                    string query = schema.Query;
                    
                    var results = await client.ExecuteReaderAsync(query);

                    var refreshProperties = new List<Property>();

            
            
                    foreach (var row in results)
                    {
                        var property = new Property(){};
                        
                        foreach (var field in row.Schema.Fields)
                        {
                            property.Name = field.Name;
                            property.Id = field.Name;
                            
                            property.Type = GetType(field.Type);
                            property.TypeAtSource = field.Type;
                            
                            property.IsKey = false; 
                            property.IsNullable = true;
                        }
                        refreshProperties.Add(property);
                    }
                    
                    schema.Properties.Clear();
                    schema.Properties.AddRange(refreshProperties);

                    yield return await AddSampleAndCount(clientFactory, schema, sampleSize);
                }
            }
            finally
            {
                //noop
            }
        }
    }
}