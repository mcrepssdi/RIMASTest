using System.Data;
using System.Data.SqlClient;
using Dapper;
using Newtonsoft.Json;

namespace RIMASTest;

public class Worker : BackgroundService
{

    private string queueName = "AddressBookQueue";
    private SqlConnection conn;
    private string connStr = "Server=localhost;Database=MessageQue;Trusted_Connection=True;";

    public Worker()
    {
        conn = new SqlConnection("Server=localhost;Database=MessageQue;Trusted_Connection=True;");
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            Console.WriteLine($"Worker running at: {DateTimeOffset.Now}");
            Run();
            await Task.Delay(1000, stoppingToken);
        }
    }

    public void Run()
    {
        SqlCommand command = new();
        try
        {
            string sql = "RECEIVE CAST(message_body AS VARCHAR(MAX)) AS message_body FROM dbo.[AddressBookQueue]";
            command = new(sql, conn);
            conn.Open();

            SqlDataReader reader = command.ExecuteReader();
            while (reader.Read())
            {
                string data = reader[0].ToString();
                List<AMaster> amasters = JsonConvert.DeserializeObject<List<AMaster>>(data);
                foreach (var master in amasters)
                {
                    Console.WriteLine(master.ToString());
                }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
        finally
        {
            command.Dispose();
            conn.Close();
        }
    }
}