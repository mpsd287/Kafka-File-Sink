public interface IMessageHandler
{
    bool ShouldProcess(string message);
}

public class MessageHandler : IMessageHandler
{
    public bool ShouldProcess(string message)
    {
        return !string.IsNullOrWhiteSpace(message);
    }
}
