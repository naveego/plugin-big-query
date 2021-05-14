using System.Collections.Generic;
using Naveego.Sdk.Plugins;
using PluginBigQuery.DataContracts;

namespace PluginBigQuery.API.Replication
{
    public static partial class Replication 
    {
        public static ReplicationTable ConvertSchemaToReplicationTable(Schema schema, string schemaName,
            string tableName)
        {
            var table = new ReplicationTable
            {
                SchemaName = schemaName,
                TableName = tableName,
                Columns = new List<ReplicationColumn>()
            };
            
            foreach (var property in schema.Properties)
            {
                var column = new ReplicationColumn
                {
                    ColumnName = property.Name,
                    DataType = string.IsNullOrWhiteSpace(property.TypeAtSource)? GetType(property.Type): property.TypeAtSource,
                    PrimaryKey = false
                };
                
                table.Columns.Add(column);
            }

            return table;
        }
        
        private static string GetType(PropertyType dataType)
        {
            switch (dataType)
            {
                case PropertyType.Datetime:
                    return "string";
                case PropertyType.Date:
                    return "string";
                case PropertyType.Time:
                    return "string";
                case PropertyType.Integer:
                    return "int64";
                case PropertyType.Decimal:
                    return "float64";
                case PropertyType.Float:
                    return "float64";
                case PropertyType.Bool:
                    return "bool";
                case PropertyType.Blob:
                    return "blob";
                case PropertyType.String:
                    return "string";
                case PropertyType.Text:
                    return "string";
                default:
                    return "string";
            }
        }
    }
}