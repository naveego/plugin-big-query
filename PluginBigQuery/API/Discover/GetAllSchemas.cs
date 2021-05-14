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

        
        
        public static async IAsyncEnumerable<Schema> GetAllSchemas(IClientFactory clientFactory, int sampleSize = 5)
        {
            var client = clientFactory.GetClient();
            
            string db = client.GetDefaultDatabase();
            string query = String.Format(GetAllTablesAndColumnsQuery, db);

            var result = await client.ExecuteReaderAsync(query);

            Schema schema = null;
            var currentSchemaId = "";
            foreach (var row in result)
            {
                var schemaId = 
                    $"{Utility.Utility.GetSafeName(row[TableSchema].ToString(), '`')}.{Utility.Utility.GetSafeName(row[TableName].ToString(), '`')}";
                if (schemaId != currentSchemaId)
                {
                    // return previous schema
                    if (schema != null)
                    {
                        // get sample and count
                        yield return await AddSampleAndCount(clientFactory, schema, sampleSize);
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
                    
                    var property = new Property
                    {
                        Id = row[ColumnName].ToString(),
                        Name = row[ColumnName].ToString(),
                        IsKey = row[ColumnKey].ToString() == "1",
                        IsNullable = row[IsNullable].ToString() == "YES",
                        Type = GetType(row[DataType].ToString()),
                         TypeAtSource = GetTypeAtSource(row[DataType].ToString(), 0)
                    };
                    
                    schema?.Properties.Add(property);
                }
                if (schema != null)
                {
                    // get sample and count
                    yield return await AddSampleAndCount(clientFactory, schema, sampleSize);
                }
            }
        }

        private static async Task<Schema> AddSampleAndCount(IClientFactory clientFactory, Schema schema,
            int sampleSize)
        {
            // add sample and count
            var records = Read.Read.ReadRecords(clientFactory, schema).Take(sampleSize);
            schema.Sample.AddRange(await records.ToListAsync());
            schema.Count = await GetCountOfRecords(clientFactory, schema);

            return schema;
        }

        public static PropertyType GetType(string dataType)
        {
            switch (dataType.ToLower())
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
                case "int64":
                    return PropertyType.Integer;
                case "numeric":
                case "bignumeric":
                case "decimal":
                case "bigdecimal":
                    return PropertyType.Decimal;
                case "float":
                case "float64":
                case "double":
                    return PropertyType.Float;
                case "boolean":
                case "bool":
                    return PropertyType.Bool;
                case "blob":
                case "mediumblob":
                case "longblob":
                    return PropertyType.Blob;
                case "char":
                case "varchar":
                case "tinytext":
                case "bytes":
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