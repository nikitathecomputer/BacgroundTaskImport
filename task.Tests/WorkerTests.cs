using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.PostgreSql;
using Xunit;
using System.Threading.Tasks;
using Npgsql;

using task.Data;
using task.Models;

namespace task.Tests;

public class WorkerTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgresContainer;
    private string _connectionString = null!;
    private readonly string _tempJsonDir;
    private readonly string _tempJsonPath;

    public WorkerTests()
    {
        _postgresContainer = new PostgreSqlBuilder()
            .WithImage("postgres:15")
            .WithDatabase("testdb")
            .WithUsername("testuser")
            .WithPassword("testpass")
            .Build();

        _tempJsonDir = Path.Combine(AppContext.BaseDirectory, "files");
        _tempJsonPath = Path.Combine(_tempJsonDir, "terminals.json");
    }

    public async Task InitializeAsync()
    {
        await _postgresContainer.StartAsync();
        _connectionString = _postgresContainer.GetConnectionString();

        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        const string createTableSql = @"
            CREATE TABLE IF NOT EXISTS ""Offices"" (
                ""Id"" SERIAL PRIMARY KEY,
                ""Code"" TEXT,
                ""CityCode"" INTEGER NOT NULL,
                ""Uuid"" TEXT,
                ""Type"" TEXT,
                ""CountryCode"" TEXT NOT NULL,
                ""Latitude"" DOUBLE PRECISION NOT NULL,
                ""Longitude"" DOUBLE PRECISION NOT NULL,
                ""AddressRegion"" TEXT,
                ""AddressCity"" TEXT,
                ""AddressStreet"" TEXT,
                ""AddressHouseNumber"" TEXT,
                ""AddressApartment"" INTEGER,
                ""WorkTime"" TEXT NOT NULL,
                ""Phones"" JSONB
            )";

        await using var cmd = new NpgsqlCommand(createTableSql, connection);
        await cmd.ExecuteNonQueryAsync();

        Directory.CreateDirectory(_tempJsonDir);
    }

    public async Task DisposeAsync()
    {
        await _postgresContainer.StopAsync();

        if (File.Exists(_tempJsonPath))
            File.Delete(_tempJsonPath);
        if (Directory.Exists(_tempJsonDir))
            Directory.Delete(_tempJsonDir, recursive: true);
    }


    [Fact]
    public async Task LoadOfficesFromJsonAsync_ValidFile_ReturnsOffices()
    {
        // Arrange
        var logger = NullLogger<Worker>.Instance;
        var configuration = new ConfigurationBuilder().Build();
        var worker = new Worker(logger, configuration);

        var testJson = @"{
            ""city"": [
                {
                ""name"": ""Тестовый город"",
                ""cityID"": 12345,
                ""terminals"": {
                    ""terminal"": [
                    {
                        ""id"": ""999"",
                        ""address"": ""ул. Тестовая, 1"",
                        ""latitude"": ""55.7558"",
                        ""longitude"": ""37.6176"",
                        ""isPVZ"": true,
                        ""isOffice"": false,
                        ""calcSchedule"": {
                        ""derival"": ""пн-пт: 09:00-18:00""
                        },
                        ""phones"": [
                        {
                            ""number"": ""+7 (123) 456-78-90"",
                            ""comment"": ""тестовый""
                        }
                        ]
                    }
                    ]
                }
                }
            ]
            }";

        var dir = Path.Combine(AppContext.BaseDirectory, "files");
        Directory.CreateDirectory(dir);
        var filePath = Path.Combine(dir, "terminals.json");
        await File.WriteAllTextAsync(filePath, testJson);

        var method = typeof(Worker).GetMethod("LoadOfficesFromJsonAsync", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        Assert.NotNull(method);

        // Act
        var task = (Task<List<Office>>)method.Invoke(worker, new object[] { CancellationToken.None })!;
        var offices = await task;

        // Assert
        Assert.Single(offices);
        var office = offices[0];
        Assert.Equal("999", office.Code);
        Assert.Equal(12345, office.CityCode);
        Assert.Equal(OfficeType.PVZ, office.Type);
        Assert.Equal("Тестовый город", office.AddressCity);
        Assert.Equal("ул. Тестовая, 1", office.AddressStreet);
        Assert.Equal(55.7558, office.Coordinates.Latitude);
        Assert.Equal(37.6176, office.Coordinates.Longitude);
        Assert.Equal("пн-пт: 09:00-18:00", office.WorkTime);
        Assert.NotNull(office.Phones);
        Assert.Equal("+7 (123) 456-78-90", office.Phones.PhoneNumber);
        Assert.Equal("тестовый", office.Phones.Additional);

        File.Delete(filePath);
        Directory.Delete(dir, recursive: true);
    }

    [Fact]
    public async Task ImportTerminalsAsync_ValidFile_InsertsDataIntoDatabase()
    {
        // Arrange
        var testJson = @"{
            ""city"": [
                {
                ""name"": ""Тестовый город"",
                ""cityID"": 12345,
                ""terminals"": {
                    ""terminal"": [
                    {
                        ""id"": ""999"",
                        ""address"": ""ул. Тестовая, 1"",
                        ""latitude"": ""55.7558"",
                        ""longitude"": ""37.6176"",
                        ""isPVZ"": true,
                        ""isOffice"": false,
                        ""calcSchedule"": {
                        ""derival"": ""пн-пт: 09:00-18:00""
                        },
                        ""phones"": [
                        {
                            ""number"": ""+7 (123) 456-78-90"",
                            ""comment"": ""тестовый""
                        }
                        ]
                    }
                    ]
                }
                }
            ]
        }";

        await File.WriteAllTextAsync(_tempJsonPath, testJson);

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["ConnectionStrings:DefaultConnection"] = _connectionString
            })
            .Build();

        var logger = NullLogger<Worker>.Instance;
        var worker = new Worker(logger, config);

        // Act
        var method = typeof(Worker).GetMethod("ImportTerminalsAsync", 
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance);
        Assert.NotNull(method);

        var task = (Task)method.Invoke(worker, new object[] { CancellationToken.None })!;
        await task;

        // Assert
        await using var connection = new NpgsqlConnection(_connectionString);
        await connection.OpenAsync();

        await using var countCmd = new NpgsqlCommand("SELECT COUNT(*) FROM \"Offices\"", connection);
        var count = (long)await countCmd.ExecuteScalarAsync();
        Assert.Equal(1, count);

        await using var selectCmd = new NpgsqlCommand(@"
            SELECT 
                ""Code"", ""CityCode"", ""Type"", ""CountryCode"", 
                ""Latitude"", ""Longitude"", ""AddressCity"", ""AddressStreet"", 
                ""WorkTime"", ""Phones""
            FROM ""Offices"" 
            WHERE ""Code"" = '999'", connection);
        await using var reader = await selectCmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());

        Assert.Equal("999", reader.GetString(0));
        Assert.Equal(12345, reader.GetInt32(1));
        Assert.Equal("PVZ", reader.GetString(2));
        Assert.Equal("RU", reader.GetString(3));
        Assert.Equal(55.7558, reader.GetDouble(4));
        Assert.Equal(37.6176, reader.GetDouble(5));
        Assert.Equal("Тестовый город", reader.GetString(6));
        Assert.Equal("ул. Тестовая, 1", reader.GetString(7));
        Assert.Equal("пн-пт: 09:00-18:00", reader.GetString(8));

        var phonesJson = reader.GetString(9);
        using var jsonDoc = JsonDocument.Parse(phonesJson);
        var root = jsonDoc.RootElement;
        Assert.Equal("+7 (123) 456-78-90", root.GetProperty("PhoneNumber").GetString());
        Assert.Equal("тестовый", root.GetProperty("Additional").GetString());
    }
}