using System;
using System.Collections.Generic;
using Google.Cloud.BigQuery.V2;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginBigQuery.API.Factory;

namespace PluginBigQuery.API.Read
{
    public static partial class Read
    {
        public static async IAsyncEnumerable<Record> ReadRecords(IClientFactory clientFactory, Schema schema)
        {

            var client = clientFactory.GetClient();
            
            try
            {

                var query = schema.Query;

                if (string.IsNullOrWhiteSpace(query))
                {
                    query = $"SELECT * FROM {schema.Id}";
                }

                BigQueryResults results = null;
                try
                {
                    results = await client.ExecuteReaderAsync(query);
                }
                catch (Exception e)
                {
                    Logger.Error(e, e.Message);
                    yield break;
                }

                var i = 0;
                foreach (var row in results)
                {
                    var recordMap = new Dictionary<string, object>();
                    
                    foreach (var property in schema.Properties)
                    {
                        try
                        {
                            switch (property.Type)
                            {
                                case PropertyType.String:
                                case PropertyType.Text:
                                case PropertyType.Decimal:
                                    recordMap[property.Id] = row[property.Id].ToString();
                                    break;
                                default:
                                    recordMap[property.Id] = row[property.Name];
                                    break;
                            }
                        }
                        catch (Exception e)
                        {
                            Logger.Error(e, $"No column with property Id: {property.Id}");
                            Logger.Error(e, e.Message);
                            recordMap[property.Id] = null;
                        }
                    }
                    var record = new Record
                    {
                        Action = Record.Types.Action.Upsert,
                        DataJson = JsonConvert.SerializeObject(recordMap)
                    };

                    yield return record;
                }
               
            }
            finally
            {
                //noop
            }
        }
    }
}