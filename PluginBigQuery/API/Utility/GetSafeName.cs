namespace PluginBigQuery.API.Utility
{
    public static partial class Utility
    {
        public static string GetSafeName(string unsafeName, char escapeChar = '`', bool isForColumn = false)
        {
            if (isForColumn)
            {
                escapeChar = '"';
                
                return $"{escapeChar}" +
                       $"{unsafeName}"
                           .Replace(" ", "_")+
                       $"{escapeChar}";
            }
            return $"{escapeChar}{unsafeName}{escapeChar}";
        }
    }
}