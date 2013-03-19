namespace RavenDbWcfHost
{
    using System.Runtime.Serialization;

    [DataContract]
    public class WcfData
    {
        public int Id { get; set; }

        public long Counter { get; set; }
    }
}