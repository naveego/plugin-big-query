namespace PluginBigQuery.API.Utility
{
    public static partial class Utility
    {
        public static string GetSafeName(string unsafeName, char escapeChar = '`', bool replaceInvalidChars = false)
        {
            if (replaceInvalidChars)
            {
                return $"{escapeChar}{unsafeName}{escapeChar}".Replace(" ", "_");
            }
            return $"{escapeChar}{unsafeName}{escapeChar}";
        }
    }
}