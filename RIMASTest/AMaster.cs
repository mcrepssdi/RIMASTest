
namespace RIMASTest;

public class AMaster
{
    public string AccountNo { get; set; }
    public string Action { get; set; }
    public DateTime LastUpdated { get; set; }
    public string Handle { get; set; } 
    
    public override string ToString()
    {
        return $"AccountNo: {AccountNo}\tAction:{Action}\tHandle:{Handle}\tLastUpdated:{LastUpdated:yyyy-MM-dd HH:m:s tt zzz}";
    }
}