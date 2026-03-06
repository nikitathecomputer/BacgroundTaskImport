using System.Diagnostics;
using System.Text.Json;
using System.Text.Json.Serialization;
using Npgsql;

using task.Models;

namespace task;

public class Worker(
    ILogger<Worker> logger,
    IConfiguration configuration) : BackgroundService
{
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() }
    };

    private readonly TimeZoneInfo _mskZone = GetMoscowTimeZone();

    /// <summary>
    /// Определяет часовой пояс Москвы кросс-платформенным способом.
    /// </summary>
    /// <returns>
    /// Объект <see cref="TimeZoneInfo"/>, представляющий московское стандартное время.
    /// </returns>
    private static TimeZoneInfo GetMoscowTimeZone()
    {
        try { return TimeZoneInfo.FindSystemTimeZoneById("Russian Standard Time"); }
        catch { return TimeZoneInfo.FindSystemTimeZoneById("Europe/Moscow"); }
    }

    /// <summary>
    /// Выполняет фоновую службу непрерывно, ожидая следующего запланированного времени импорта.
    /// </summary>
    /// <param name="cancelationToken">Токен отмены для остановки службы.</param>
    /// <returns>
    /// Задача, представляющая асинхронную операцию.
    /// </returns>
    protected override async Task ExecuteAsync(CancellationToken cancelationToken)
    {
        logger.LogInformation("Служба импорта терминалов запущена");

        while (!cancelationToken.IsCancellationRequested)
        {
            try
            {
                var now = DateTimeOffset.UtcNow;
                var nextRun = GetNextRunTime(now, TimeSpan.FromHours(2));
                var delay = nextRun - now;

                logger.LogInformation("Следующий импорт запланирован на {NextRun:yyyy-MM-dd HH:mm:ss}",
                    TimeZoneInfo.ConvertTime(nextRun, _mskZone));

                await Task.Delay(delay, cancelationToken);
                await ImportTerminalsAsync(cancelationToken);
            }
            catch (OperationCanceledException)
            {
                logger.LogInformation("Импорт прерван при остановке службы");
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Необработанная ошибка в цикле импорта");
                await Task.Delay(TimeSpan.FromMinutes(5), cancelationToken);
            }
        }
    }

    /// <summary>
    /// Вычисляет следующее наступление указанного времени суток в московском часовом поясе.
    /// </summary>
    /// <param name="fromUtc">Текущее время в UTC.</param>
    /// <param name="targetTimeOfDay">Желаемое время суток.</param>
    /// <returns>
    /// Следующее UTC <see cref="DateTimeOffset"/>, когда наступит целевое время в Москве.
    /// </returns>
    private DateTimeOffset GetNextRunTime(DateTimeOffset fromUtc, TimeSpan targetTimeOfDay)
    {
        var nowInMsk = TimeZoneInfo.ConvertTime(fromUtc, _mskZone);
        var nextInMsk = nowInMsk.Date.Add(targetTimeOfDay);
        if (nowInMsk > nextInMsk)
            nextInMsk = nextInMsk.AddDays(1);

        return TimeZoneInfo.ConvertTime(nextInMsk, _mskZone, TimeZoneInfo.Utc);
    }

    /// <summary>
    /// Оркестрирует полный процесс импорта: загружает офисы из JSON, очищает таблицу,
    /// создаёт временную таблицу, копирует данные через COPY и вставляет в основную таблицу.
    /// </summary>
    /// <param name="cancellationToken">Токен отмены для прерывания импорта.</param>
    /// <returns>
    /// Задача, представляющая асинхронную операцию импорта.
    /// </returns>
    /// <exception cref="Exception">Перебрасывает любое исключение после логирования.</exception>
    private async Task ImportTerminalsAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Начало импорта терминалов");
        var stopwatch = Stopwatch.StartNew();

        await using var connection = await CreateDbConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        try
        {
            var offices = await LoadOfficesFromJsonAsync(cancellationToken);
            if (offices.Count == 0)
            {
                logger.LogWarning("Нет терминалов для импорта");
                return;
            }

            await TruncateOfficesTableAsync(connection, cancellationToken);
            await CreateTempTableAsync(connection, cancellationToken);
            await CopyToTempTableAsync(connection, offices, cancellationToken);
            var inserted = await InsertFromTempToOfficesAsync(connection, cancellationToken);

            await transaction.CommitAsync(cancellationToken);

            stopwatch.Stop();
            logger.LogInformation("Импорт завершён: сохранено {Count} терминалов за {ElapsedMs} мс",
                inserted, stopwatch.ElapsedMilliseconds);
        }
        catch (Exception ex)
        {
            await transaction.RollbackAsync(cancellationToken);
            stopwatch.Stop();
            logger.LogError(ex, "Ошибка импорта после {ElapsedMs} мс", stopwatch.ElapsedMilliseconds);
            throw;
        }
    }

    /// <summary>
    /// Читает и разбирает файл terminals.json, преобразуя вложенную структуру в список объектов <see cref="Office"/>.
    /// </summary>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>
    /// Список офисов, извлечённых из JSON-файла, или пустой список, если файл отсутствует или повреждён.
    /// </returns>
    private async Task<List<Office>> LoadOfficesFromJsonAsync(CancellationToken cancellationToken)
    {
        var filePath = Path.Combine(AppContext.BaseDirectory, "files", "terminals.json");
        if (!File.Exists(filePath))
        {
            logger.LogError("Файл {FilePath} не найден", filePath);
            return [];
        }

        var json = await File.ReadAllTextAsync(filePath, cancellationToken);
        using var document = JsonDocument.Parse(json);
        var root = document.RootElement;

        if (!root.TryGetProperty("city", out var cityArray))
        {
            logger.LogError("В JSON отсутствует корневой массив 'city'");
            return [];
        }

        var offices = new List<Office>();

        foreach (var cityElem in cityArray.EnumerateArray())
        {
            var cityName = cityElem.GetProperty("name").GetString() ?? "";

            if (!cityElem.TryGetProperty("cityID", out var cityIdProp) || cityIdProp.ValueKind == JsonValueKind.Null)
            {
                logger.LogWarning("Пропущен город {CityName}: отсутствует cityID", cityName);
                continue;
            }
            int cityId = cityIdProp.GetInt32();

            if (!cityElem.TryGetProperty("terminals", out var terminalsObj) ||
                !terminalsObj.TryGetProperty("terminal", out var terminalArray))
                continue;

            foreach (var termElem in terminalArray.EnumerateArray())
            {
                var office = new Office
                {
                  Coordinates = new Coordinates()  
                };

                if (termElem.TryGetProperty("id", out var idProp))
                    office.Code = idProp.GetString();

                office.CityCode = cityId;

                var isPvz = termElem.TryGetProperty("isPVZ", out var pvzProp) && pvzProp.GetBoolean();
                var isOffice = termElem.TryGetProperty("isOffice", out var offProp) && offProp.GetBoolean();
                office.Type = isPvz ? OfficeType.PVZ : OfficeType.WAREHOUSE;

                office.CountryCode = "RU";

                if (termElem.TryGetProperty("latitude", out var latProp) &&
                    termElem.TryGetProperty("longitude", out var lngProp))
                {
                    double.TryParse(latProp.GetString(), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var lat);
                    double.TryParse(lngProp.GetString(), System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out var lng);
                    office.Coordinates.Latitude = lat;
                    office.Coordinates.Longitude = lng;
                }

                office.AddressCity = cityName;
                if (termElem.TryGetProperty("address", out var addrProp))
                    office.AddressStreet = addrProp.GetString();

                if (termElem.TryGetProperty("calcSchedule", out var calcElem))
                {
                    office.WorkTime = calcElem.TryGetProperty("derival", out var derivalProp)
                        ? derivalProp.GetString() ?? ""
                        : calcElem.TryGetProperty("arrival", out var arrivalProp)
                            ? arrivalProp.GetString() ?? ""
                            : "";
                }

                if (termElem.TryGetProperty("phones", out var phonesArray) && phonesArray.GetArrayLength() > 0)
                {
                    var firstPhone = phonesArray.EnumerateArray().First();
                    var phoneNumber = firstPhone.TryGetProperty("number", out var numProp) ? numProp.GetString() : null;
                    var comment = firstPhone.TryGetProperty("comment", out var commProp) ? commProp.GetString() : null;
                    if (!string.IsNullOrEmpty(phoneNumber))
                    {
                        office.Phones = new Phone
                        {
                            PhoneNumber = phoneNumber,
                            Additional = comment
                        };
                    }
                }

                offices.Add(office);
            }
        }

        logger.LogInformation("Загружено {Count} офисов из JSON", offices.Count);
        return offices;
    }

    /// <summary>
    /// Открывает новое подключение к PostgreSQL, используя строку подключения из конфигурации.
    /// </summary>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>
    /// Открытое соединение <see cref="NpgsqlConnection"/>.
    /// </returns>    
    private async Task<NpgsqlConnection> CreateDbConnectionAsync(CancellationToken cancellationToken)
    {
        var connectionString = configuration.GetConnectionString("DefaultConnection");
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        logger.LogDebug("Подключение к БД открыто");
        return connection;
    }

    /// <summary>
    /// Очищает таблицу Offices.
    /// </summary>
    /// <param name="connection">Открытое подключение к PostgreSQL.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    private async Task TruncateOfficesTableAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var cmd = new NpgsqlCommand("TRUNCATE TABLE \"Offices\" RESTART IDENTITY", connection);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
        logger.LogInformation("Таблица Offices очищена");
    }

    /// <summary>
    /// Создаёт временную таблицу "temp_offices", повторяющую структуру основной таблицы "Offices",
    /// включая колонку JSONB для хранения телефона. Таблица автоматически удаляется в конце транзакции.
    /// </summary>
    /// <param name="connection">Открытое подключение к PostgreSQL.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    private async Task CreateTempTableAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
            CREATE TEMP TABLE temp_offices (
                row_num SERIAL PRIMARY KEY,
                code text,
                city_code integer,
                uuid text,
                type text,
                country_code text,
                latitude double precision,
                longitude double precision,
                address_region text,
                address_city text,
                address_street text,
                address_house_number text,
                address_apartment integer,
                work_time text,
                phones jsonb
            ) ON COMMIT DROP
            """;
        await using var cmd = new NpgsqlCommand(sql, connection);
        await cmd.ExecuteNonQueryAsync(cancellationToken);
        logger.LogDebug("Временная таблица temp_offices создана");
    }

    /// <summary>
    /// Выполняет массовую вставку данных офисов во временную таблицу с использованием протокола COPY.
    /// Объекты телефонов сериализуются в JSON перед вставкой.
    /// </summary>
    /// <param name="connection">Открытое подключение к PostgreSQL.</param>
    /// <param name="offices">Список офисов для вставки.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    private async Task CopyToTempTableAsync(NpgsqlConnection connection, List<Office> offices, CancellationToken cancellationToken)
    {
        await using var writer = connection.BeginBinaryImport(
            "COPY temp_offices (code, city_code, uuid, type, country_code, latitude, longitude, address_region, address_city, address_street, address_house_number, address_apartment, work_time, phones) FROM STDIN (FORMAT BINARY)");

        foreach (var office in offices)
        {
            await writer.StartRowAsync(cancellationToken);
            await writer.WriteAsync(office.Code, NpgsqlTypes.NpgsqlDbType.Text, cancellationToken);
            await writer.WriteAsync(office.CityCode, NpgsqlTypes.NpgsqlDbType.Integer, cancellationToken);
            await writer.WriteAsync(office.Uuid, NpgsqlTypes.NpgsqlDbType.Text, cancellationToken);
            await writer.WriteAsync(office.Type?.ToString(), NpgsqlTypes.NpgsqlDbType.Text, cancellationToken);
            await writer.WriteAsync(office.CountryCode, NpgsqlTypes.NpgsqlDbType.Text, cancellationToken);
            await writer.WriteAsync(office.Coordinates.Latitude, NpgsqlTypes.NpgsqlDbType.Double, cancellationToken);
            await writer.WriteAsync(office.Coordinates.Longitude, NpgsqlTypes.NpgsqlDbType.Double, cancellationToken);
            await writer.WriteAsync(office.AddressRegion, NpgsqlTypes.NpgsqlDbType.Text, cancellationToken);
            await writer.WriteAsync(office.AddressCity, NpgsqlTypes.NpgsqlDbType.Text, cancellationToken);
            await writer.WriteAsync(office.AddressStreet, NpgsqlTypes.NpgsqlDbType.Text, cancellationToken);
            await writer.WriteAsync(office.AddressHouseNumber, NpgsqlTypes.NpgsqlDbType.Text, cancellationToken);
            await writer.WriteAsync(office.AddressApartment, NpgsqlTypes.NpgsqlDbType.Integer, cancellationToken);
            await writer.WriteAsync(office.WorkTime, NpgsqlTypes.NpgsqlDbType.Text, cancellationToken);

            string phonesJson = office.Phones is null ? "null" : JsonSerializer.Serialize(office.Phones, _jsonOptions);
            await writer.WriteAsync(phonesJson, NpgsqlTypes.NpgsqlDbType.Jsonb, cancellationToken);
        }

        await writer.CompleteAsync(cancellationToken);
        logger.LogDebug("Данные скопированы во временную таблицу");
    }

    /// <summary>
    /// Переносит данные из временной таблицы в основную таблицу Offices.
    /// </summary>
    /// <param name="connection">Открытое подключение к PostgreSQL.</param>
    /// <param name="cancellationToken">Токен отмены.</param>
    /// <returns>Количество вставленных строк.</returns>
    private async Task<int> InsertFromTempToOfficesAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
            INSERT INTO "Offices" 
            ("Code", "CityCode", "Uuid", "Type", "CountryCode", "Latitude", "Longitude", 
             "AddressRegion", "AddressCity", "AddressStreet", "AddressHouseNumber", "AddressApartment", "WorkTime", "Phones")
            SELECT 
                code, city_code, uuid, type, country_code, latitude, longitude, 
                address_region, address_city, address_street, address_house_number, address_apartment, work_time, phones
            FROM temp_offices
            ORDER BY row_num
            """;
        await using var cmd = new NpgsqlCommand(sql, connection);
        var inserted = await cmd.ExecuteNonQueryAsync(cancellationToken);
        logger.LogInformation("Сохранено {Count} новых терминалов", inserted);
        return inserted;
    }

    /// <summary>
    /// Останавливает фоновую службу.
    /// </summary>
    /// <param name="cancellationToken">Токен отмены.</param>
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Служба импорта терминалов останавливается");
        await base.StopAsync(cancellationToken);
    }
}
