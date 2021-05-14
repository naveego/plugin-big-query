using System;
using System.IO;
using PluginBigQuery.API.Discover;

namespace PluginBigQuery.Helper
{
    public class Settings
    {
        public string ProjectId { get; set; }
        
        public string JsonFilePath { get; set; }
        
        public string DefaultDatabase { get; set; }
        
        /// <summary>
        /// Validates the settings input object
        /// </summary>
        /// <exception cref="Exception"></exception>
        public void Validate()
        {
            if (String.IsNullOrEmpty(ProjectId))
            {
                throw new Exception("The Project ID property must be set");
            }
            
            if (String.IsNullOrEmpty(JsonFilePath))
            {
                throw new Exception("The JsonFilePath property must be set");
            }
            
            if (string.IsNullOrEmpty(DefaultDatabase))
            {
                throw new Exception("The default database property must be set");
            }
            
            if (!File.Exists(JsonFilePath))
            {
                throw new Exception($"No JSON file found at given path: {JsonFilePath}");
            }
        }
    }
}