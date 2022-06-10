using System.Data.SqlClient;
using System.Net.Http.Headers;
using Dapper;
using Newtonsoft.Json;

namespace RIMASTest;

public class Worker : BackgroundService
{
    private readonly SqlConnection conn;
    private const string ConnStr = "Server=localhost;Database=MessageQue;Trusted_Connection=True;";
    private const string ServiceBrokerSql = "RECEIVE CAST(message_body AS VARCHAR(MAX)) AS message_body FROM dbo.[AddressBookQueue]";
    private const string DeleteSql = "DELETE FROM dbo.AMasterSample WHERE Handle = @Handle";
    private const string SaiWepApiUrl = "corp-saisdiint.fw.sdi.local:8100/srvc80test/Customer/GetCustomer";
    private const string ApiToken = "Tst4269";
    static HttpClient client = new HttpClient();
    
    
    public Worker()
    {
        conn = new SqlConnection(ConnStr);
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            Console.WriteLine($"Worker running at: {DateTimeOffset.Now}");
            try
            {
                Run();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            await Task.Delay(1000, stoppingToken);
        }
    }
    
    public async void Run()
    {
        try
        {
            conn.Open();
            await ReadMessage();
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }
        finally
        {
            conn.Close();
        }
    }

    private Task ReadMessage()
    {
        IEnumerable<dynamic>? query = conn.Query(ServiceBrokerSql);
        foreach(IDictionary<string, object> row in query)
        {
            foreach (KeyValuePair<string, object> item in row)
            {
                List<AMaster>? amasters = JsonConvert.DeserializeObject<List<AMaster>>((string)item.Value);
                amasters?.ForEach(Action);
            }
        }

        return Task.CompletedTask;
    }

    private async void Action(AMaster p)
    {
        await GetCustomer(p.AccountNo);

        DynamicParameters dp = new();
        dp.Add("@Handle", p.Handle);
        int rows = await conn.ExecuteAsync(DeleteSql, dp);
        
        Console.WriteLine(p + $"\tDeleted: {Convert.ToBoolean(rows).ToString()}");
    }

    private async Task GetCustomer(string accountNo)
    {
        // Update port # in the following line.
        client.BaseAddress = new Uri(SaiWepApiUrl);

        client.DefaultRequestHeaders.Accept.Clear();
        client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        client.DefaultRequestHeaders.Add("API-Token", ApiToken);
        HttpResponseMessage response = await client.PostAsync(SaiWepApiUrl,
            new StringContent(JsonConvert.SerializeObject(new CustomerPost
                {DataEnvironmentName = "dl4osc", CustomerNo = "123456"})));
        if (response.IsSuccessStatusCode)
        {
           /* Do Something here */
        }
    }
}