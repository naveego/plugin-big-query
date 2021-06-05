namespace PluginBigQuery.API.Utility
{
    public static partial class Utility
    {
        public static string GetSafeName(string unsafeName, char escapeChar = '`', bool replaceUnsafeCharacters = false)
        {
            if (replaceUnsafeCharacters)
            {
                return $"{escapeChar}" +
                       $"{unsafeName}"
                           .Replace(" ", "_")
                           .Replace("'", "\\'") +
                       $"{escapeChar}";
            }
            return $"{escapeChar}{unsafeName}{escapeChar}";
        }
    }
}