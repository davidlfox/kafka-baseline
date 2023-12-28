using Newtonsoft.Json;

public class StructuredMessage
{
    [JsonRequired]
    [JsonProperty("id")]
    public int Id { get; set; }

    [JsonProperty("text")]
    public string Text { get; set; } = string.Empty;

    [JsonRequired]
    [JsonProperty("timestamp")]
    public DateTime Timestamp { get; set; }
}