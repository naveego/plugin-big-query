using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Grpc.Core;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginBigQuery.DataContracts;
using PluginBigQuery.Helper;
using Xunit;
using Record = Naveego.Sdk.Plugins.Record;

namespace PluginBigQueryTest.Plugin
{
    public class PluginIntegrationTest
    {
        private Settings GetSettings()
        {
            return new Settings
            {
                // populate for tests
                DefaultDatabase = "",
                ProjectId = "",
                JsonFilePath = @""
            };
        }

        private ConnectRequest GetConnectSettings()
        {
            var settings = GetSettings();

            return new ConnectRequest
            {
                SettingsJson = JsonConvert.SerializeObject(settings),
                OauthConfiguration = new OAuthConfiguration(),
                OauthStateJson = ""
            };
        }

        private Schema GetTestSchema(string id = "`testdata`.`table1`", string name = "testdata.table1", string query = "")
        {
            return new Schema
            {
                Id = id,
                Name = name,
                Query = query,
                Properties =
                {
                    new Property
                    {
                        Id = "Id",
                        Name = "Id",
                        Type = PropertyType.Integer,
                        IsKey = true
                    },
                    new Property
                    {
                        Id = "Name",
                        Name = "Name",
                        Type = PropertyType.String
                    }
                }
            };
        }

        [Fact]
        public async Task ConnectSessionTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginBigQuery.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = GetConnectSettings();
            var disconnectRequest = new DisconnectRequest();

            // act
            var response = client.ConnectSession(request);
            var responseStream = response.ResponseStream;
            var records = new List<ConnectResponse>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
                client.Disconnect(disconnectRequest);
            }

            // assert
            Assert.Single(records);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ConnectTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginBigQuery.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var request = GetConnectSettings();

            // act
            var response = client.Connect(request);

            // assert
            Assert.IsType<ConnectResponse>(response);
            Assert.Equal("", response.SettingsError);
            Assert.Equal("", response.ConnectionError);
            Assert.Equal("", response.OauthError);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasAllTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginBigQuery.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.All,
                SampleSize = 10
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Equal(8, response.Schemas.Count);
            
            var schema = response.Schemas[0];
            Assert.Equal($"`testdata`.`NaveegoReplicationMetaData`", schema.Id);
            Assert.Equal("testdata.NaveegoReplicationMetaData", schema.Name);
            Assert.Equal($"", schema.Query);
            Assert.Equal(2, schema.Sample.Count);
            Assert.Equal(5, schema.Properties.Count);
            
            
            var property = schema.Properties[0];
            Assert.Equal("NaveegoJobId", property.Id);
            Assert.Equal("NaveegoJobId", property.Name);
            Assert.Equal("", property.Description);
            Assert.Equal(PropertyType.String, property.Type);
            Assert.False(property.IsKey);
            Assert.True(property.IsNullable);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasRefreshTableTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginBigQuery.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                SampleSize = 10,
                ToRefresh = {GetTestSchema("`testdata`.`table1`", "testdata.table1")}
                // ToRefresh = {GetTestSchema("`testdata`.`emptytable`", "testdata.emptytable")}
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Single(response.Schemas);

            var schema = response.Schemas[0];
            Assert.Equal($"`testdata`.`table1`", schema.Id);
            Assert.Equal("testdata.table1", schema.Name);
            Assert.Equal($"", schema.Query);
            Assert.Equal(10, schema.Sample.Count);
            Assert.Equal(6, schema.Properties.Count);

            //Properties is null here also
            var property = schema.Properties[0];
            Assert.Equal("col1", property.Id);
            Assert.Equal("col1", property.Name);
            Assert.Equal("", property.Description);
            Assert.Equal(PropertyType.Integer, property.Type);
            Assert.False(property.IsKey);
            Assert.True(property.IsNullable);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasRefreshQueryTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginBigQuery.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                SampleSize = 10,
                ToRefresh = {GetTestSchema("`testdata`.`table1`", "testdata.table1", $"SELECT * FROM `testdata`.`table1`")}
            };

            // act
            client.Connect(connectRequest);
            var response = client.DiscoverSchemas(request);

            // assert
            Assert.IsType<DiscoverSchemasResponse>(response);
            Assert.Single(response.Schemas);

            var schema = response.Schemas[0];
            Assert.Equal($"`testdata`.`table1`", schema.Id);
            Assert.Equal("testdata.table1", schema.Name);
            Assert.Equal($"SELECT * FROM `testdata`.`table1`", schema.Query);
            Assert.Equal(10, schema.Sample.Count);
            
            
            Assert.Equal(6, schema.Properties.Count);

            var property = schema.Properties[0];
            Assert.Equal("col1", property.Id);
            Assert.Equal("col1", property.Name);
            Assert.Equal("", property.Description);
            Assert.Equal(PropertyType.String, property.Type);
            Assert.False(property.IsKey);
            Assert.True(property.IsNullable);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task DiscoverSchemasRefreshQueryBadSyntaxTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginBigQuery.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                SampleSize = 10,
                ToRefresh = {GetTestSchema("testdata.table1", "testdata.table1", $"bad syntax")}
            };

            // act
            client.Connect(connectRequest);

            try
            {
                var response = client.DiscoverSchemas(request);
            }
            catch (Exception e)
            {
                // assert
                Assert.IsType<RpcException>(e);
                
                Regex rgx = new Regex("Job.*contained errors", RegexOptions.Compiled);
                bool passed = rgx.IsMatch(e.Message);
                Assert.True(passed);
            }

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }
        
        [Fact]
        public async Task ReadStreamTableSchemaTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginBigQuery.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var schema = GetTestSchema("`testdata`.`table1`", "testdata.table1", "SELECT * FROM `testdata`.`table1`");

            var connectRequest = GetConnectSettings();

            var schemaRequest = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = {schema}
            };

            var request = new ReadRequest()
            {
                DataVersions = new DataVersions
                {
                    JobId = "test"
                },
                JobId = "test",
            };

            // act
            client.Connect(connectRequest);
            var schemasResponse = client.DiscoverSchemas(schemaRequest);
            request.Schema = schemasResponse.Schemas[0];

            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(10, records.Count);

            var record = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[9].DataJson);
            Assert.Equal("64", record["col1"]);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }
        [Fact]
        public async Task ReadStreamQuerySchemaTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginBigQuery.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var schema = GetTestSchema("`testdata`,`table1`", "testdata.table1", $"SELECT * FROM `testdata`.`table1`");

            var connectRequest = GetConnectSettings();

            var schemaRequest = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = {schema}
            };

            var request = new ReadRequest()
            {
                DataVersions = new DataVersions
                {
                    JobId = "test"
                },
                JobId = "test",
            };

            // act
            client.Connect(connectRequest);

            var schemasResponse = client.DiscoverSchemas(schemaRequest);
            request.Schema = schemasResponse.Schemas[0];

            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }
            
            
            // assert
            Assert.Equal(10, records.Count);

            var record = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[9].DataJson);
            Assert.Equal("64", record["col1"]);
            // Assert.Equal("101", record["col2"]);
            // Assert.Equal("Kevin O'Leary", record["col3"]);
            // Assert.Equal("202", record["col4"]);
            // Assert.True(Boolean.Parse(record["col5"].ToString()));
            // Assert.Equal(DateTime.Parse("2003-01-06T00:00:00"), record["col6"]);
            
            
            // var record2 = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[1].DataJson);
            // Assert.Equal(null, record2["col1"]);
            // Assert.Equal("-4.8", record2["col2"]);
            // Assert.Equal("back\\this", record2["col3"]);
            // Assert.Equal(null, record2["col4"]);
            // Assert.False(Boolean.Parse(record2["col5"].ToString()));
            // Assert.Equal(DateTime.Parse("1001-01-01T00:00:00"), record2["col6"]);
            //
            // var record3 = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[2].DataJson);
            // Assert.Equal(null, record3["col1"]);
            // Assert.Equal("-4.8", record3["col2"]);
            // Assert.Equal("apostrophe'''THAT'", record3["col3"]);
            // Assert.Equal(null, record3["col4"]);
            // Assert.False(Boolean.Parse(record3["col5"].ToString()));
            // Assert.Equal(DateTime.Parse("1001-01-01T00:00:00"), record3["col6"]);
            //
            // var record4 = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[4].DataJson);
            // Assert.Equal("3", record4["col1"]);
            // Assert.Equal("-4.8", record4["col2"]);
            // Assert.Equal("tricky\\\''this", record4["col3"]);
            // Assert.Equal(null, record4["col4"]);
            // Assert.False(Boolean.Parse(record4["col5"].ToString()));
            // Assert.Equal(DateTime.Parse("0001-01-01T00:00:00"), record4["col6"]);
            //
            // var record5 = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[3].DataJson);
            // Assert.Equal("1", record5["col1"]);
            // Assert.Equal("2.2", record5["col2"]);
            // Assert.Equal("", record5["col3"]);
            // Assert.Equal(null, record5["col4"]);
            // Assert.False(Boolean.Parse(record5["col5"].ToString()));
            // Assert.Equal(DateTime.Parse("1970-01-01T00:00:00"), record5["col6"]);
            
            
            // var record6 = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[5].DataJson);
            // var record7 = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[6].DataJson);
            // var record8 = JsonConvert.DeserializeObject<Dictionary<string, object>>(records[7].DataJson);
            
            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReadStreamLimitTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginBigQuery.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var schema = GetTestSchema("`testdata`.`table1`", "testdata.table1");

            var connectRequest = GetConnectSettings();

            var schemaRequest = new DiscoverSchemasRequest
            {
                Mode = DiscoverSchemasRequest.Types.Mode.Refresh,
                ToRefresh = {schema}
            };

            var request = new ReadRequest()
            {
                DataVersions = new DataVersions
                {
                    JobId = "test"
                },
                JobId = "test",
                Limit = 10
            };

            // act
            client.Connect(connectRequest);
            var schemasResponse = client.DiscoverSchemas(schemaRequest);
            request.Schema = schemasResponse.Schemas[0];

            var response = client.ReadStream(request);
            var responseStream = response.ResponseStream;
            var records = new List<Record>();

            while (await responseStream.MoveNext())
            {
                records.Add(responseStream.Current);
            }

            // assert
            Assert.Equal(10, records.Count);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task PrepareWriteTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginBigQuery.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var request = new PrepareWriteRequest()
            {
                Schema = GetTestSchema(), 
                CommitSlaSeconds = 1,
                Replication = new ReplicationWriteRequest
                {
                    SettingsJson = JsonConvert.SerializeObject(new ConfigureReplicationFormData
                    {
                        GoldenTableName = @"gr_test",
                        VersionTableName = @"vr_test"
                    })
                },
                DataVersions = new DataVersions
                {
                    JobId = "jobUnitTest",
                    ShapeId = "shapeUnitTest",
                    JobDataVersion = 2,
                    ShapeDataVersion = 3
                }
            };

            // act
            client.Connect(connectRequest);
            var response = client.PrepareWrite(request);

            // assert
            Assert.IsType<PrepareWriteResponse>(response);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }

        [Fact]
        public async Task ReplicationWriteTest()
        {
            // setup
            Server server = new Server
            {
                Services = {Publisher.BindService(new PluginBigQuery.Plugin.Plugin())},
                Ports = {new ServerPort("localhost", 0, ServerCredentials.Insecure)}
            };
            server.Start();

            var port = server.Ports.First().BoundPort;

            var channel = new Channel($"localhost:{port}", ChannelCredentials.Insecure);
            var client = new Publisher.PublisherClient(channel);

            var connectRequest = GetConnectSettings();

            var prepareWriteRequest = new PrepareWriteRequest()
            {
                Schema = GetTestSchema(),
                CommitSlaSeconds = 1000,
                Replication = new ReplicationWriteRequest
                {
                    SettingsJson = JsonConvert.SerializeObject(new ConfigureReplicationFormData
                    {
                        GoldenTableName = "gr_test",
                        VersionTableName = "vr_test"
                    })
                },
                DataVersions = new DataVersions
                {
                    JobId = "jobUnitTest",
                    ShapeId = "shapeUnitTest",
                    JobDataVersion = 1,
                    ShapeDataVersion = 1
                }
            };

            var records = new List<Record>()
            {
                {
                    new Record
                    {
                        Action = Record.Types.Action.Upsert,
                        CorrelationId = "test",
                        RecordId = "record1",
                        DataJson = "{\"Id\":1,\"Name\":\"The Kevin O'Leary Company\"}",
                        Versions =
                        {
                            new RecordVersion
                            {
                                RecordId = "version1",
                                DataJson = "{\"Id\":1,\"Name\":\"The Kevin O'Leary Company\"}",
                            }
                        }
                    }
                }
            };

            var recordAcks = new List<RecordAck>();

            // act
            client.Connect(connectRequest);
            client.PrepareWrite(prepareWriteRequest);

            using (var call = client.WriteStream())
            {
                var responseReaderTask = Task.Run(async () =>
                {
                    while (await call.ResponseStream.MoveNext())
                    {
                        var ack = call.ResponseStream.Current;
                        recordAcks.Add(ack);
                    }
                });

                foreach (Record record in records)
                {
                    await call.RequestStream.WriteAsync(record);
                }

                await call.RequestStream.CompleteAsync();
                await responseReaderTask;
            }

            // assert
            Assert.Single(recordAcks);
            Assert.Equal("", recordAcks[0].Error);
            Assert.Equal("test", recordAcks[0].CorrelationId);

            // cleanup
            await channel.ShutdownAsync();
            await server.ShutdownAsync();
        }
    }
}