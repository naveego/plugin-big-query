using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using Newtonsoft.Json;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {
        public static async Task UpsertRecordAsync(IClientFactory clientFactory,
            ReplicationTable table,
            Dictionary<string, object> recordMap)
        {
            var client = clientFactory.GetClient();

            // delete from table
            // where
            
            try
            {
                var primaryKey = table.Columns.Find(c => c.PrimaryKey);
                var primaryValue = recordMap[primaryKey.ColumnName];
                if (primaryKey.Serialize)
                {
                    primaryValue = JsonConvert.SerializeObject(primaryValue);
                }

                if (!await RecordExistsAsync(clientFactory, table, primaryValue.ToString()))
                {
                    var querySb =
                        new StringBuilder(
                            $"INSERT INTO {Utility.Utility.GetSafeName(table.SchemaName, '`', true)}.{Utility.Utility.GetSafeName(table.TableName, '`', true)}(");
                    foreach (var column in table.Columns)
                    {
                        querySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName, '`', true)},");
                    }
                    
                    querySb.Length--;
                    querySb.Append(") VALUES (");
                    
                    foreach (var column in table.Columns)
                    {
                        if (recordMap.ContainsKey(column.ColumnName))
                        {
                            var rawValue = recordMap[column.ColumnName];
                            if (column.Serialize)
                            {
                                rawValue = JsonConvert.SerializeObject(rawValue);
                            }
                    
                            switch (column.DataType.ToLower())
                            {
                                case "string":
                                case "datetime":
                                case "date":
                                case "time":
                                case "timestamp":
                                    querySb.Append(rawValue != null
                                        ? $"'{Utility.Utility.GetSafeString(rawValue.ToString(), "'", "\\'")}',"
                                        : $"NULL,");
                                    break;
                                default:
                                    querySb.Append(rawValue != null
                                        ? $"{Utility.Utility.GetSafeString(rawValue.ToString(), "'", "\\'")},"
                                        : $"NULL,");
                                    break;
                            }
                        }
                        else
                        {
                            querySb.Append($"NULL,");
                        }
                    }
                    
                    querySb.Length--;
                    querySb.Append(");");
                    
                    var query = querySb.ToString();
                    
                    Logger.Debug($"Insert record query: {query}");
                    
                    await client.ExecuteReaderAsync(query);
                }
                else
                {
                    var querySb =
                        new StringBuilder(
                            $"UPDATE {Utility.Utility.GetSafeName(table.SchemaName, '`', true)}.{Utility.Utility.GetSafeName(table.TableName, '`', true)} SET ");
                    foreach (var column in table.Columns)
                    {
                        if (!column.PrimaryKey)
                        {
                            if (recordMap.ContainsKey(column.ColumnName))
                            {
                                var rawValue = recordMap[column.ColumnName];
                                if (column.Serialize)
                                {
                                    rawValue = JsonConvert.SerializeObject(rawValue);
                                }
                                
                                querySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName, '`', true)}=");
                                
                                switch (column.DataType.ToLower())
                                {
                                            
                                    case "string":
                                    case "datetime":
                                    case "date":
                                    case "time":
                                    case "timestamp":
                                        querySb.Append(rawValue != null
                                            ? $"'{Utility.Utility.GetSafeString(rawValue.ToString(), "'", "\\'")}',"
                                            : $"NULL,");
                                        break;
                                    default:
                                        querySb.Append(rawValue != null
                                            ? $"{Utility.Utility.GetSafeString(rawValue.ToString(), "'", "\\'")},"
                                            : $"NULL,");
                                        break;
                                }
                            }
                            else
                            {
                                querySb.Append($"NULL,");
                            }
                        }
                    }
                    
                    querySb.Length--;
                    
                    querySb.Append($" WHERE {primaryKey.ColumnName} = '{primaryValue}'");
                    
                    var query = querySb.ToString();
                    
                    await client.ExecuteReaderAsync(query);
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, $"Error Upsert Record: {e.Message}");
                throw;
            }
            
            // try
            // {
            //     // try to insert
            //     var querySb =
            //         new StringBuilder(
            //             $"INSERT INTO {Utility.Utility.GetSafeName(table.SchemaName, '`', true)}.{Utility.Utility.GetSafeName(table.TableName, '`', true)}(");
            //     foreach (var column in table.Columns)
            //     {
            //         querySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName, '`', true)},");
            //     }
            //
            //     querySb.Length--;
            //     querySb.Append(") VALUES (");
            //
            //     foreach (var column in table.Columns)
            //     {
            //         if (recordMap.ContainsKey(column.ColumnName))
            //         {
            //             var rawValue = recordMap[column.ColumnName];
            //             if (column.Serialize)
            //             {
            //                 rawValue = JsonConvert.SerializeObject(rawValue);
            //             }
            //
            //             switch (column.DataType.ToLower())
            //             {
            //                 case "string":
            //                 case "datetime":
            //                 case "date":
            //                 case "time":
            //                 case "timestamp":
            //                     querySb.Append(rawValue != null
            //                         ? $"'{Utility.Utility.GetSafeString(rawValue.ToString(), "'", "\\'")}',"
            //                         : $"NULL,");
            //                     break;
            //                 default:
            //                     querySb.Append(rawValue != null
            //                         ? $"{Utility.Utility.GetSafeString(rawValue.ToString(), "'", "\\'")},"
            //                         : $"NULL,");
            //                     break;
            //             }
            //         }
            //         else
            //         {
            //             querySb.Append($"NULL,");
            //         }
            //     }
            //
            //     querySb.Length--;
            //     querySb.Append(");");
            //
            //     var query = querySb.ToString();
            //
            //     Logger.Debug($"Insert record query: {query}");
            //
            //     await client.ExecuteReaderAsync(query);
            // }
            // catch (Exception e)
            // {
            //     try
            //     {
            //         // update if it failed
            //         var querySb =
            //             new StringBuilder(
            //                 $"UPDATE {Utility.Utility.GetSafeName(table.SchemaName, '`')}.{Utility.Utility.GetSafeName(table.TableName, '`')} SET ");
            //         foreach (var column in table.Columns)
            //         {
            //             if (!column.PrimaryKey)
            //             {
            //                 if (recordMap.ContainsKey(column.ColumnName))
            //                 {
            //                     var rawValue = recordMap[column.ColumnName];
            //                     if (column.Serialize)
            //                     {
            //                         rawValue = JsonConvert.SerializeObject(rawValue);
            //                     }
            //
            //                     if (rawValue != null)
            //                     {
            //                         querySb.Append(
            //                             $"{Utility.Utility.GetSafeName(column.ColumnName, '`', true)}='{Utility.Utility.GetSafeString(rawValue.ToString(), "'", "\\'")}',");
            //                     }
            //                     else
            //                     {
            //                         querySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName, '`', true)}=NULL,");
            //                     }
            //                 }
            //                 else
            //                 {
            //                     querySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName, '`', true)}=NULL,");
            //                 }
            //             }
            //         }
            //
            //         querySb.Length--;
            //
            //         var primaryKey = table.Columns.Find(c => c.PrimaryKey);
            //         var primaryValue = recordMap[primaryKey.ColumnName];
            //         if (primaryKey.Serialize)
            //         {
            //             primaryValue = JsonConvert.SerializeObject(primaryValue);
            //         }
            //
            //         querySb.Append($" WHERE {primaryKey.ColumnName} = '{primaryValue}'");
            //
            //         var query = querySb.ToString();
            //
            //         await client.ExecuteReaderAsync(query);
            //     }
            //     catch (Exception exception)
            //     {
            //         Logger.Error(e, $"Error Insert: {e.Message}");
            //         Logger.Error(exception, $"Error Update: {exception.Message}");
            //         throw;
            //     }
            //     finally
            //     {
            //         // await conn.CloseAsync();
            //     }
            // }
            // finally
            // {
            //     //await conn.CloseAsync();
            // }
        }
    }
}