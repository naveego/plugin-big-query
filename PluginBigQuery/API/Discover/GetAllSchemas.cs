using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginBigQuery.API.Factory;

namespace PluginBigQuery.API.Discover
{
    public static partial class Discover
    {
        private const string TableName = "TABLE_NAME";
        private const string TableSchema = "TABLE_SCHEMA";
        private const string TableType = "TABLE_TYPE";
        private const string ColumnName = "COLUMN_NAME";
        private const string DataType = "DATA_TYPE";
        private const string ColumnKey = "COLUMN_KEY";
        private const string IsNullable = "IS_NULLABLE";
        private const string CharacterMaxLength = "CHARACTER_MAXIMUM_LENGTH";
        
        

        private const string GetAllTablesAndColumnsQuery = @"
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

      ORDER BY t.TABLE_NAME;";

        
        //public static async IAsyncEnumerable<Schema> GetAllSchemas(IClientFactory connFactory, int sampleSize = 5)
        public static async IAsyncEnumerable<Schema> GetAllSchemas(IClientFactory clientFactory, int sampleSize = 5)
        {
            //Just data tables, no system schemas
            //var conn = connFactory.GetConnection();

            var client = clientFactory.GetClient();

            string projectId = client.GetProjectId();
            string query = String.Format(GetTableAndColumnsQuery, "testdata");

            var result = await client.ExecuteReaderAsync(GetAllTablesAndColumnsQuery);

            Schema bq_schema = null;
            var bq_currentSchemaId = "";
            
            foreach (var row in result)
            {
                var bq_schemaId = result.TableReference.TableId;
                    //$"{Utility.Utility.GetSafeName(reader.GetValueById(TableSchema).ToString(), '`')}.{Utility.Utility.GetSafeName(reader.GetValueById(TableName).ToString(), '`')}";
                if (bq_schemaId != bq_currentSchemaId)
                {
                    // return previous schema
                    if (bq_schema != null)
                    {
                        // get sample and count
                        yield return await AddSampleAndCount(clientFactory, bq_schema, sampleSize);
                    }

                    // start new schema
                    bq_currentSchemaId = bq_schemaId;
                    var parts = DecomposeSafeName(bq_currentSchemaId).TrimEscape();
                    bq_schema = new Schema
                    {
                        Id = bq_currentSchemaId,
                        Name = $"{parts.Schema}.{parts.Table}",
                        Properties = { },
                        DataFlowDirection = Schema.Types.DataFlowDirection.Read
                    };
                }
            }
            
            
            #region MySQL Method
            try
            {
                
                
                await conn.OpenAsync();

                var cmd = connFactory.GetCommand(GetAllTablesAndColumnsQuery, conn);
                var reader = await cmd.ExecuteReaderAsync();

                Schema schema = null;
                var currentSchemaId = "";
                while (await reader.ReadAsync())
                {
                    var schemaId =
                        $"{Utility.Utility.GetSafeName(reader.GetValueById(TableSchema).ToString(), '`')}.{Utility.Utility.GetSafeName(reader.GetValueById(TableName).ToString(), '`')}";
                    if (schemaId != currentSchemaId)
                    {
                        // return previous schema
                        if (schema != null)
                        {
                            // get sample and count
                            yield return await AddSampleAndCount(connFactory, schema, sampleSize);
                        }

                        // start new schema
                        currentSchemaId = schemaId;
                        var parts = DecomposeSafeName(currentSchemaId).TrimEscape();
                        schema = new Schema
                        {
                            Id = currentSchemaId,
                            Name = $"{parts.Schema}.{parts.Table}",
                            Properties = { },
                            DataFlowDirection = Schema.Types.DataFlowDirection.Read
                        };
                    }

                    // add column to schema
                    var property = new Property
                    {
                        Id = $"`{reader.GetValueById(ColumnName)}`",
                        Name = reader.GetValueById(ColumnName).ToString(),
                        IsKey = reader.GetValueById(ColumnKey).ToString() == "PRI",
                        IsNullable = reader.GetValueById(IsNullable).ToString() == "YES",
                        Type = GetType(reader.GetValueById(DataType).ToString()),
                        TypeAtSource = GetTypeAtSource(reader.GetValueById(DataType).ToString(),
                            reader.GetValueById(CharacterMaxLength))
                    };
                    
                    schema?.Properties.Add(property);
                }

                if (schema != null)
                {
                    // get sample and count
                    yield return await AddSampleAndCount(connFactory, schema, sampleSize);
                }
            }
            finally
            {
                await conn.CloseAsync();
            }
            #endregion
        }

        private static async Task<Schema> AddSampleAndCount(IConnectionFactory connFactory, Schema schema,
            int sampleSize)
        {
            // add sample and count
            var records = Read.Read.ReadRecords(connFactory, schema).Take(sampleSize);
            schema.Sample.AddRange(await records.ToListAsync());
            schema.Count = await GetCountOfRecords(connFactory, schema);

            return schema;
        }
        
        private static async Task<Schema> AddSampleAndCount(IClientFactory clientFactory, Schema schema,
            int sampleSize)
        {
            // add sample and count
            var records = Read.Read.ReadRecords(clientFactory, schema).Take(sampleSize);
            schema.Sample.AddRange(await records.ToListAsync());
            schema.Count = await GetCountOfRecords(clientFactory, schema);
            
            
            //schema.Sample = Store sample records.
            //schema.Count = total count of records

            return schema;
        }

        public static PropertyType GetType(string dataType)
        {
            switch (dataType)
            {
                case "datetime":
                case "timestamp":
                    return PropertyType.Datetime;
                case "date":
                    return PropertyType.Date;
                case "time":
                    return PropertyType.Time;
                case "tinyint":
                case "smallint":
                case "mediumint":
                case "int":
                case "bigint":
                    return PropertyType.Integer;
                case "numeric":
                case "decimal":
                    return PropertyType.Decimal;
                case "float":
                case "double":
                    return PropertyType.Float;
                case "boolean":
                    return PropertyType.Bool;
                case "blob":
                case "mediumblob":
                case "longblob":
                    return PropertyType.Blob;
                case "char":
                case "varchar":
                case "tinytext":
                    return PropertyType.String;
                case "text":
                case "mediumtext":
                case "longtext":
                    return PropertyType.Text;
                default:
                    return PropertyType.String;
            }
        }

        private static string GetTypeAtSource(string dataType, object maxLength)
        {
            return maxLength != null ? $"{dataType}({maxLength})" : dataType;
        }
    }
}