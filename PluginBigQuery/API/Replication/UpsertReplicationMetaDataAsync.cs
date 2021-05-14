using System;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using Newtonsoft.Json;
using PluginBigQuery.API.Factory;
using PluginBigQuery.API.Utility;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {
        private static readonly string InsertMetaDataQuery = $@"INSERT INTO {{0}}.{{1}} 
(
{Constants.ReplicationMetaDataJobId}
, {Constants.ReplicationMetaDataRequest}
, {Constants.ReplicationMetaDataReplicatedShapeId}
, {Constants.ReplicationMetaDataReplicatedShapeName}
, {Constants.ReplicationMetaDataTimestamp})
VALUES (
'{{2}}'
, '{{3}}'
, '{{4}}'
, '{{5}}'
, '{{6}}'
)";

        private static readonly string UpdateMetaDataQuery = $@"UPDATE {{0}}.{{1}}
SET 
{Constants.ReplicationMetaDataRequest} = '{{2}}'
, {Constants.ReplicationMetaDataReplicatedShapeId} = '{{3}}'
, {Constants.ReplicationMetaDataReplicatedShapeName} = '{{4}}'
, {Constants.ReplicationMetaDataTimestamp} = '{{5}}'
WHERE {Constants.ReplicationMetaDataJobId} = '{{6}}'";

        public static async Task UpsertReplicationMetaDataAsync(IClientFactory clientFactory, ReplicationTable table,
            ReplicationMetaData metaData)
        {

            var client = clientFactory.GetClient();

            try
            {
                if (!await RecordExistsAsync(clientFactory, table, metaData.Request.DataVersions.JobId))
                {
                    var query = string.Format(InsertMetaDataQuery,
                        Utility.Utility.GetSafeName(table.SchemaName, '`'),
                        Utility.Utility.GetSafeName(table.TableName, '`'),
                        metaData.Request.DataVersions.JobId,
                        JsonConvert.SerializeObject(metaData.Request).Replace("\\", "\\\\"),
                        metaData.ReplicatedShapeId,
                        metaData.ReplicatedShapeName,
                        metaData.Timestamp
                    );
                    
                    await client.ExecuteReaderAsync(query);
                }
                else
                {
                    // update if found
                    
                    var query = string.Format(UpdateMetaDataQuery,
                        Utility.Utility.GetSafeName(table.SchemaName, '`'),
                        Utility.Utility.GetSafeName(table.TableName, '`'),
                        metaData.Request.DataVersions.JobId,
                        JsonConvert.SerializeObject(metaData.Request).Replace("\\", "\\\\"),
                        metaData.ReplicatedShapeId,
                        metaData.ReplicatedShapeName,
                        metaData.Timestamp
                    );
                    
                    await client.ExecuteReaderAsync(query);
                }
            }
            catch (Exception e)
            {
                try
                {
                    // update if it failed
                    var query = string.Format(UpdateMetaDataQuery,
                        Utility.Utility.GetSafeName(table.SchemaName, '`'),
                        Utility.Utility.GetSafeName(table.TableName, '`'),
                        JsonConvert.SerializeObject(metaData.Request).Replace("\\", "\\\\"),
                        metaData.ReplicatedShapeId,
                        metaData.ReplicatedShapeName,
                        metaData.Timestamp,
                        metaData.Request.DataVersions.JobId
                    );

                    await client.ExecuteReaderAsync(query);

                }
                catch (Exception exception)
                {
                    Logger.Error(e, $"Error Insert: {e.Message}");
                    Logger.Error(exception, $"Error Update: {exception.Message}");
                    throw;
                }
                finally
                {
                    //noop
                }
            }
            finally
            {
                //noop
            }
        }
    }
}