namespace Qunar.TC.Qmq.Client.Dubbo
{
    internal interface IRequestHandler
    {
        object Handle(object msg);
    }
}
