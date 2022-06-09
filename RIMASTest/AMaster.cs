using System.Xml;

namespace RIMASTest;

public class AMaster
{
    public string AccountNo { get; set; }
    public string Action { get; set; }
    public DateTime LastUpdated { get; set; }
    public string Id { get; set; } 
    
    public override string ToString()
    {
        return $"AccountNo: {AccountNo}\tAction:{Action}\tHandle:{Id}\tDateTime: {LastUpdated:yyyy-MM-dd}";
    }
}