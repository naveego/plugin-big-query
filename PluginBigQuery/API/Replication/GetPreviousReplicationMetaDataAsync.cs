using System;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginBigQuery.API.Factory;
using PluginBigQuery.DataContracts;
using Constants = PluginBigQuery.API.Utility.Constants;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication
    {
        private static readonly string GetMetaDataQuery = @"SELECT * FROM {0}.{1} WHERE {2} = '{3}'";

        public static async Task<ReplicationMetaData> GetPreviousReplicationMetaDataAsync(
            IClientFactory clientFactory,
            string jobId,
            ReplicationTable table)
        {
            var client = clientFactory.GetClient();

            string query = string.Format(GetMetaDataQuery,
                Utility.Utility.GetSafeName(table.SchemaName, '`'),
                Utility.Utility.GetSafeName(table.TableName, '`'),
                Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId),
                jobId);
            

            try
            {
                ReplicationMetaData replicationMetaData = null;

                // ensure replication metadata table
                await EnsureTableAsync(clientFactory, table);
                
                // check if metadata exists
                var bqReader = await client.ExecuteReaderAsync(query);

                foreach (var row in bqReader)
                {
                    var request = JsonConvert.DeserializeObject<PrepareWriteRequest>(
                        row[Constants.ReplicationMetaDataRequest].ToString());
                    var shapeName = row[Constants.ReplicationMetaDataReplicatedShapeName].ToString();
                    var shapeId = row[Constants.ReplicationMetaDataReplicatedShapeId].ToString();
                    var timestamp = DateTime.Parse(row[Constants.ReplicationMetaDataTimestamp].ToString());
                    
                     replicationMetaData = new ReplicationMetaData
                    {
                        Request = request,
                        ReplicatedShapeName = shapeName,
                        ReplicatedShapeId = shapeId,
                        Timestamp = timestamp
                    };
                }

                return replicationMetaData;
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message);
                throw;
            }
            finally
            {
                //noop
            }
        }
    }
}