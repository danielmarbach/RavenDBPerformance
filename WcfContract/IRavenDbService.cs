namespace RavenDbWcfHost
{
    using System.ServiceModel;

    [ServiceContract]
    public interface IRavenDbService
    {
        [OperationContract]
        void Store(WcfData data);
    }
}