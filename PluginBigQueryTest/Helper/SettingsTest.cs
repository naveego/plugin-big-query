using System;
using PluginBigQuery.Helper;
using Xunit;

namespace PluginBigQueryTest.Helper
{
    public class SettingsTest
    {
        [Fact]
        public void ValidateValidTest()
        {
            // setup
            var settings = new Settings
            {
                DefaultDatabase = "",
                ProjectId = "",
                JsonFilePath = @""
            };

            // act
            settings.Validate();

            // assert
        }

        [Fact]
        public void ValidateNoDefaultDatabaseTest()
        {
            // setup
            var settings = new Settings
            {
                DefaultDatabase = null,
                ProjectId = "first-test-project-312212",
                JsonFilePath = @"C:\"
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The default database property must be set", e.Message);
        }
        
        [Fact]
        public void ValidateNoProjectIdTest()
        {
            // setup
            var settings = new Settings
            {
                DefaultDatabase = "testdata",
                ProjectId = null,
                JsonFilePath = @"C:\"
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The Project ID property must be set", e.Message);
        }
        
        [Fact]
        public void ValidateNoJsonFilePathTest()
        {
            // setup
            var settings = new Settings
            {
                DefaultDatabase = "testdata",
                ProjectId = "first-test-project-312212",
                JsonFilePath = null
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The JsonFilePath property must be set", e.Message);
        }
        [Fact]
        public void ValidateNoJsonFileTest()
        {
            // setup
            var settings = new Settings
            {
                DefaultDatabase = "testdata",
                ProjectId = "first-test-project-312212",
                JsonFilePath = @"C:\"
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("No JSON file found at given path", e.Message);
        }
    }
}