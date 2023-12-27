using System.Net.Http;
using System.Threading.Tasks;
using System.IO;
using Microsoft.VisualBasic.FileIO;
using System;
using Npgsql;

namespace Lab10
{
    public class DatabaseConfig
    {
        private const string Dsn = "Host=localhost;Port=5432;Database=MyData;Username=postgres;Password=postgres";

        public static string GetDsn() //возвращает строку подключения к бд
        {
            return Dsn;
        }
    }
    public class HTTPRequest  //выполняем GET-запросы
    {
        public static async Task<string> Request(string url)
        {
            using (HttpClient httpClient = new HttpClient()) //для отправки запросов
            {
                try
                {
                    HttpResponseMessage response = await httpClient.GetAsync(url); //выполнение GET-запроса

                    if (response.IsSuccessStatusCode)
                    {
                        return await response.Content.ReadAsStringAsync();
                    }
                    else
                    {
                        throw new HttpRequestException($"HTTP Error: {response.StatusCode} - {response.ReasonPhrase}");
                    }
                }
                catch (HttpRequestException e)
                {
                    throw new HttpRequestException($"Request error: {e.Message}");
                }
            }
        }
    }

    public class Parser
    {
        public static async Task<string[]> Parse(string CSVString) //асинхронный парсинг CSV-строки
        {
            using (StringReader stringReader = new StringReader(CSVString))
            {

                using (TextFieldParser textFieldParser = new TextFieldParser(stringReader)) //для парсинга текстовых полей
                {
                    textFieldParser.TextFieldType = FieldType.Delimited; //тип - разделённые
                    textFieldParser.SetDelimiters(","); //запятая в качестве распределителя

                    textFieldParser.ReadFields();

                    string[] rows = textFieldParser.ReadFields(); //читает содержимое

                    return rows;
                }
            }
        }
    }

    public class TickerPrice
    {
        public static async Task<double> GetTodayPrice(string ticker) //цена за сегодня
        {
            DateTime today = DateTime.Today;
            DateTime yesterday = today.AddDays(-1);

            long todayUnixTimestamp = ((DateTimeOffset)today).ToUnixTimeSeconds();
            long yesterdayUnixTimestamp = ((DateTimeOffset)yesterday).ToUnixTimeSeconds();

            string url = $"https://query1.finance.yahoo.com/v7/finance/download/" +
                            $"{ticker}?period1={yesterdayUnixTimestamp}&period2={todayUnixTimestamp}" +
                            "&interval=1d&events=history&includeAdjustedClose=true";

            string response = await HTTPRequest.Request(url); //GET-запрос

            var parsedResponse = await Parser.Parse(response);
            // Console.WriteLine(parsedResponse[1]);
            return Convert.ToDouble(parsedResponse[1].Replace('.', ','));
        }

        public static async Task<double> GetYesterdayPrice(string ticker) //получение цены за прошлые дни
        {
            DateTime yesterday = DateTime.Today.AddDays(-1);
            DateTime twoDaysAgo = yesterday.AddDays(-1);

            long yesterdayUnixTimestamp = ((DateTimeOffset)yesterday).ToUnixTimeSeconds();
            long twoDaysAgoUnixTimestamp = ((DateTimeOffset)twoDaysAgo).ToUnixTimeSeconds();

            string url = $"https://query1.finance.yahoo.com/v7/finance/download/" +
                         $"{ticker}?period1={twoDaysAgoUnixTimestamp}&period2={yesterdayUnixTimestamp}" +
                         "&interval=1d&events=history&includeAdjustedClose=true";

            string response = await HTTPRequest.Request(url);

            var parsedResponse = await Parser.Parse(response);

            return Convert.ToDouble(parsedResponse[1].Replace('.', ','));
        }
    }



    internal class Program
    {
        private static readonly string path = "C:\\Users\\user\\Desktop\\ticker.txt";
        private static readonly object lockObject = new object(); //лбъект для многопоточного доступа
        public static async Task Main(string[] args)
        {
            DateTime today = DateTime.Today;

            NpgsqlConnection connection = new NpgsqlConnection(DatabaseConfig.GetDsn()); //подключаемся к бд PostgreSQL
            await connection.OpenAsync();

            string createTickersTable = "CREATE TABLE IF NOT EXISTS Tickers " +
                                        "(id SERIAL PRIMARY KEY, " +
                                        "ticker VARCHAR(255));";

            string createPricesTable = "CREATE TABLE IF NOT EXISTS Prices " +
                                       "(id SERIAL PRIMARY KEY, " +
                                       "tickerid INT, " +
                                       "price DOUBLE PRECISION, " +
                                       "date VARCHAR(255));";

            string createTodaysConditionTable = "CREATE TABLE IF NOT EXISTS TodaysCondition " +
                                                "(id SERIAL PRIMARY KEY, " +
                                                "tickerid INT, " +
                                                "state VARCHAR(255));";

            NpgsqlCommand commandCreateTables = new NpgsqlCommand(createTickersTable
                                                                  + createPricesTable
                                                                  + createTodaysConditionTable, connection);
            commandCreateTables.ExecuteNonQuery(); //создание таблиц

            try
            {
                using (StreamReader reader = new StreamReader(path)) //открываем файл для чтения
                {
                    string ticker;
                    while ((ticker = reader.ReadLine()) != null)
                    {
                        double price;
                        try
                        {
                            price = await TickerPrice.GetTodayPrice(ticker); //запрос цены
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error fetching price for {ticker}: {ex.Message}");
                            continue;
                        }

                        NpgsqlConnection updateConnection = new NpgsqlConnection(DatabaseConfig.GetDsn());
                        await updateConnection.OpenAsync(); //обновление бд

                        NpgsqlCommand insertCommand = updateConnection.CreateCommand();
                        insertCommand.CommandText = "INSERT INTO Tickers (ticker) VALUES (@ticker) RETURNING id";
                        insertCommand.Parameters.AddWithValue("@ticker", ticker);
                        int tickerId = (int)insertCommand.ExecuteScalar();


                        insertCommand.CommandText = $"INSERT INTO Prices (tickerid, price, date) VALUES (@tickerId, @price, @today)";
                        insertCommand.Parameters.AddWithValue("@tickerId", tickerId);
                        insertCommand.Parameters.AddWithValue("@price", price);
                        insertCommand.Parameters.AddWithValue("@today", today.ToString("yyyy-MM-dd"));
                        insertCommand.ExecuteNonQuery();

                        updateConnection.Close();
                        await AnalyzeAndUpdateCondition(connection, ticker, price, today); //анализ цены
                       
                    }

                }
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Error reading file: {ex.Message}");
            }

            connection.Close(); //закрытие соединенеия

            NpgsqlConnection newConnection = new NpgsqlConnection(DatabaseConfig.GetDsn()); //создание нового соединения

            try
            {

                while (true)
                {
                    Console.Write("Enter ticker to retrieve its condition (or 'exit' to exit): "); //работаем до exit
                    string userInput = Console.ReadLine();

                    if (userInput.ToLower() == "exit")
                        break;

                    string ticker = userInput.ToUpper();

                    await newConnection.OpenAsync();

                    lock (lockObject)
                    {
                        try
                        {
                            int tickerId = GetTickerId(newConnection, ticker);

                            if (tickerId == 0)
                            {
                                Console.WriteLine($"Ticker '{ticker}' not found in the database.");
                                continue;
                            }

                            string selectConditionQuery = "SELECT state FROM TodaysCondition WHERE tickerid = @tickerId"; //SQL-запрос для condition
                            NpgsqlCommand selectConditionCommand = new NpgsqlCommand(selectConditionQuery, newConnection);//команда
                            selectConditionCommand.Parameters.AddWithValue("@tickerId", tickerId);

                            var condition = selectConditionCommand.ExecuteScalar(); //выполняет SQL запрос к базе данных и возвращает результат выполнения запроса в переменную condition.

                            Console.WriteLine($"Condition for {ticker}: {condition}");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error retrieving condition for {ticker}: {ex.Message}");
                        }
                    }
                    newConnection.Close();
                }
            }
            catch (IOException ex)
            {
                Console.WriteLine($"Error reading file: {ex.Message}");
            }
            connection.Close();
        }

        private static async Task AnalyzeAndUpdateCondition(NpgsqlConnection connection, string ticker, double todayPrice, DateTime today)
        {
            try
            {
                int tickerId = GetTickerId(connection, ticker);

                double yesterdayPrice = await TickerPrice.GetYesterdayPrice(ticker);

                double condition = todayPrice - yesterdayPrice;

                NpgsqlCommand insertConditionCommand = connection.CreateCommand();
                insertConditionCommand.CommandText =
                    "INSERT INTO TodaysCondition (tickerid, state) VALUES (@tickerId, @state)";
                insertConditionCommand.Parameters.AddWithValue("@tickerId", tickerId);
                insertConditionCommand.Parameters.AddWithValue("@state", condition);
                await insertConditionCommand.ExecuteNonQueryAsync();
            }
            catch (Exception err)
            {
                Console.WriteLine("error");
            }
        }

        private static int GetTickerId(NpgsqlConnection connection, string ticker)
        {
            NpgsqlCommand command = connection.CreateCommand();
            command.CommandText = $"SELECT id FROM Tickers WHERE ticker = '{ticker}'";
            command.Parameters.AddWithValue("@ticker", ticker);
            return (int)command.ExecuteScalar(); //запрос по идентификатору
        }

    }
}
