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
        //public static async IAsyncEnumerable<Record> ReadRecords(IConnectionFactory connFactory, Schema schema)
        public static async IAsyncEnumerable<Record> ReadRecords(IClientFactory clientFactory, Schema schema)
        {
            //var conn = connFactory.GetConnection();

            var client = clientFactory.GetClient();
            
            try
            {
                //await conn.OpenAsync();

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
                                    
                                    //property.Id
                                    //recordMap[property.Id] = reader.GetValueById(property.Id, '`').ToString();
                                    recordMap[property.Id] = row[i].ToString();
                                    break;
                                default:
                                    recordMap[property.Id] = row[i];
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
               
                
                //Below is original code for MySQL
                
                // var cmd = connFactory.GetCommand(query, conn);
                // IReader reader;
                //
                // try
                // {
                //     reader = await cmd.ExecuteReaderAsync();
                // }
                // catch (Exception e)
                // {
                //     Logger.Error(e, e.Message);
                //     yield break;
                // }
                //
                //
                // if (reader.HasRows())
                // {
                //     while (await reader.ReadAsync())
                //     {
                //         var recordMap = new Dictionary<string, object>();
                //
                //         foreach (var property in schema.Properties)
                //         {
                //             try
                //             {
                //                 switch (property.Type)
                //                 {
                //                     case PropertyType.String:
                //                     case PropertyType.Text:
                //                     case PropertyType.Decimal:
                //                         recordMap[property.Id] = reader.GetValueById(property.Id, '`').ToString();
                //                         break;
                //                     default:
                //                         recordMap[property.Id] = reader.GetValueById(property.Id, '`');
                //                         break;
                //                 }
                //             }
                //             catch (Exception e)
                //             {
                //                 Logger.Error(e, $"No column with property Id: {property.Id}");
                //                 Logger.Error(e, e.Message);
                //                 recordMap[property.Id] = null;
                //             }
                //         }
                //
                //         var record = new Record
                //         {
                //             Action = Record.Types.Action.Upsert,
                //             DataJson = JsonConvert.SerializeObject(recordMap)
                //         };
                //
                //         yield return record;
                //     }
                // }
            }
            finally
            {
                //await conn.CloseAsync();
            }
        }
    }
}