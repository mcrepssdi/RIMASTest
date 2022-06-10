using System.Data;
using System.Data.SqlClient;
using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Dapper;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;

namespace RIMASTest;

public class Worker : BackgroundService
{
    private readonly SqlConnection conn;
    private const string ConnStr = "Server=localhost;Database=MessageQue;Trusted_Connection=True;";
    private const string MySqlConnStr = "Server=corpsaidev.omni.lan;Database=dl4testosc;Uid=saisupport;Pwd=saisupport;";
    private const string ServiceBrokerSql = "RECEIVE CAST(message_body AS VARCHAR(MAX)) AS message_body FROM dbo.[AddressBookQueue]";
    private const string DeleteSql = "DELETE FROM dbo.AMasterSample WHERE Handle = @Handle";
    //private const string SaiWepApiUrl = "corp-saisdiint.fw.sdi.local:8100/srvc80test/Customer/GetCustomer";
    //private const string ApiToken = "Tst4269";dl4testosc
    private const string SaiSchema = "dl4testosc";
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
        // client.BaseAddress = new Uri(SaiWepApiUrl);
        //
        // client.DefaultRequestHeaders.Accept.Clear();
        // client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        // client.DefaultRequestHeaders.Add("API-Token", ApiToken);
        // HttpResponseMessage response = await client.PostAsync(SaiWepApiUrl,
        //     new StringContent(JsonConvert.SerializeObject(new CustomerPost
        //         {DataEnvironmentName = "dl4osc", CustomerNo = "123456"})));
        // if (response.IsSuccessStatusCode)
        // {
        //     /* Do Something here */
        // }
        string serviceMethod = "Customer/GetCustomer";
        (string serviceUrl, string servicePath, string token) = GetSaiServiceParams(SaiSchema);
        string endPoint = $"http://{serviceUrl}/{servicePath}/{serviceMethod}";
        Console.WriteLine($"{endPoint}");
        
        string data = JsonConvert.SerializeObject(new CustomerPost {DataEnvironmentName = SaiSchema, CustomerNo = "AAAR02"});
        string response = PostWithResponse(data, endPoint, new List<(string key, string value)>
        {
            ("API-Token", token),
            ("Content-Type", "text/xml;charset=\"utf-8\""),
        });

    }
    
    public (string serviceUrl, string servicePath, string token) GetSaiServiceParams(string schema)
    {
        Console.WriteLine($"{schema}");
        string serviceUrl = string.Empty, servicePath = string.Empty, token = string.Empty;
        StringBuilder sb = new();

        try
        {
            string serviceUrlQuery = "SELECT Value AS Property1 FROM saimaster.SAI_Webservice_Prop WHERE Property = @Property1;";
            string servicePathQuery = "SELECT Value AS Property2 FROM saimaster.SAI_Webservice_Prop WHERE Property = @Property2;";
            sb.Append("SELECT Token FROM saimaster.SAI_Webservice_Token ");
            sb.AppendLine(" WHERE Env = @Environment AND Service_Name = @WildCard AND Service_Method = @WildCard");

            DynamicParameters dp = new();
            dp.Add("@Property1", "InternalServerName");
            dp.Add("@Property2", "InternalServicePath");
            dp.Add("@Environment", schema);
            dp.Add("@WildCard", "*");

            using MySqlConnection mysqlConn = new(MySqlConnStr);
            mysqlConn.Open();
            serviceUrl = mysqlConn.QuerySingle<string>(serviceUrlQuery, dp);
            servicePath = mysqlConn.QuerySingle<string>(servicePathQuery, dp);
            token = mysqlConn.QuerySingle<string>(sb.ToString(), dp);
        }
        catch (Exception e)
        {
            Console.WriteLine(e.Message);
        }

        return (serviceUrl, servicePath, token);
    }
    
    public string PostWithResponse(string data, string url, List<(string key, string value)> headers, string contentType = "application/json")
    {
        byte[] dataStream = Encoding.UTF8.GetBytes(data);

        Console.WriteLine("Setting up http request header");
        HttpWebRequest webRequest = (HttpWebRequest)WebRequest.Create(url);

        foreach ((string key, string value) in headers)
        {
            webRequest.Headers.Add(key, value);
        }
            
        webRequest.Method = "POST";
        webRequest.ContentType = contentType;
        webRequest.ContentLength = dataStream.Length;

        Console.WriteLine("Formatting request data");
        using Stream requestStream = webRequest.GetRequestStream();
        requestStream.Write(dataStream);
        requestStream.Close();

        Console.WriteLine("Running request");
        IAsyncResult asyncResult = webRequest.BeginGetResponse(null, null);
        asyncResult.AsyncWaitHandle.WaitOne();
        using WebResponse webResponse = webRequest.EndGetResponse(asyncResult);
        using StreamReader rd = new StreamReader(webResponse.GetResponseStream() ?? throw new InvalidOperationException());
            
        Console.WriteLine("Reading response");
        string response = rd.ReadToEnd();
        Console.WriteLine($"Returning response \n {response}");

        return response;
    }
    
}