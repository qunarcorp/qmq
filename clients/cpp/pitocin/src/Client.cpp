#include "Client.hpp"
#include "Http.hpp"
#include "Ipv4Getter.hpp"
#include <unistd.h>

#include <cstring>
#include <sstream>
#include <iostream>

namespace Pitocin
{
Client::Client(uv_loop_t *loop)
    : loop(loop),
      http(loop),
      calibrator(loop),
      metaTcp(loop)
{
    // 获取本地ip
    Ipv4Getter getter;
    localIp = getter.toString();

    // 获取进程号
    pid = getpid();
}

Client::~Client()
{
    if (error)
    {
        error(0, "destruct");
    }
    uv_timer_stop(&httpTimer);
    uv_timer_stop(&heartbeatTimer);
    MetaTcp *tcp = metaTcp.getTarget();
    if (tcp != nullptr)
    {
        delete tcp;
    }
}

void Client::setBaseInfo(string &&_appCode, string &&_room, string &&_env)
{
    appCode = _appCode;
    clientId = localIp + "@" + appCode;
    room = _room;
    env = _env;
}

void Client::setErrCallback(decltype(error) callback)
{
    error = callback;
}

void Client::setHttpUrl(string &&url)
{
    http.setUrl(forward<string>(url));
}

void Client::setHttpReqInterval(uint64_t interval)
{
    reqInterval = interval;
}

void Client::start()
{
    startHttp();
    startHeartbeat();
}

void Client::publish(
    const string &subject,
    const vector<pair<string, string>> &kvList,
    function<void(string &, int)> callback)
{
    if (pubCtxPool.empty())
    {
        pubCtxPool.emplace();
    }
    PubCtx *ctx = pubCtxPool.pop();

    ctx->client = this;
    PublishMessage &msg = ctx->msg;
    msg.flag = PublishMessage::FlagHigh;
    msg.beginTs = calibrator.milliseconds();
    msg.subject = subject;
    msg.id = genMsgId();
    msg.kvList = kvList;
    /**
     * 根据余昭辉的要求,需要在每个msg的kvList中加入
     * qmq_appCode和qmq_createTIme这两个k赖兼容历史问题
     * */
    msg.kvList.push_back(make_pair("qmq_appCode", appCode));
    msg.kvList.push_back(make_pair("qmq_createTIme", to_string(calibrator.milliseconds())));

    ctx->callback = [ctx, callback](string &msgId, int code) {
        callback(msgId, code);
        ctx->client->pubCtxPool.push(ctx);
    };

    // 如何处理pubResult
    auto pubCallback = [ctx](int code, Header *header, PublishMessageResult *result) {
        switch (code)
        {
        case CodeNoBroker:
            ctx->callback(ctx->msg.id, CodeNoBroker);
            break;
        case BrokerTcp::CodeTimeout:
            ctx->callback(ctx->msg.id, CodeTimeout);
            break;
        case BrokerTcp::CodePaseError:
            ctx->callback(ctx->msg.id, CodeError);
            break;
        case BrokerTcp::CodeSuccess:
            switch (result->code)
            {
            case PublishMessageResult::CodeSuccess:
            case PublishMessageResult::CodeDuplicated:
                ctx->callback(ctx->msg.id, CodeSuccess);
                break;
            case PublishMessageResult::CodeBusy:
            case PublishMessageResult::CodeIntercepted:
            case PublishMessageResult::CodeSlave:
                ctx->callback(ctx->msg.id, (int)result->code);
                ctx->client->brokerMap.erase(ctx->broker->getInfo().addr);
                ctx->client->metaRespMapPub.erase(ctx->msg.subject);
                break;
            }
            break;
        }
    };

    // 使用BrokerTcp来调用publish
    auto brokerCallback = [ctx, pubCallback](BrokerTcp *broker) {
        if (broker == nullptr)
        {
            cout << "brokerCallback::BrokerTcp_is_nullptr" << endl;
            ctx->callback(ctx->msg.id, CodeNoBroker);
            return;
        }
        ctx->broker = broker;
        broker->publish(
            &ctx->header,
            &ctx->msg,
            pubCallback);
    };

    // 使用MetaResponse来寻找BrokerTcp
    auto metaRespCallback = [ctx, brokerCallback](MetaResponse *resp) {
        if (resp == nullptr)
        {
            cout << "metaRespCallback::MetaResponse_is_nullptr" << endl;
            ctx->callback(ctx->msg.id, CodeNoBroker);
            return;
        }
        ctx->client->getBroker(resp, brokerCallback);
    };

    getPubMetaResp(ctx->msg.subject, metaRespCallback);
}

void Client::getPubMetaResp(
    string &subject,
    function<void(MetaResponse *)> callback)
{
    bool needMetaReq = false;
    auto pair = metaRespMapPub.emplace(subject, RequestValve<MetaResponse>(loop));
    RequestValve<MetaResponse> &valve = pair.first->second;
    MetaResponse *resp = valve.getTarget();
    if (resp != nullptr)
    {
        callback(resp);
        uint64_t nowSeconds = calibrator.seconds();
        if (resp->seconds >= nowSeconds - 60)
        {
            return;
        }
        else
        {
            resp->seconds = nowSeconds - 55;
            callback = nullptr;
        }
    }
    else if (metaTcp.getTarget() == nullptr)
    {
        callback(nullptr);
        return;
    }
    // 根据需要,是否刷新metaResp
    auto task = [this, subject, callback](RequestValve<MetaResponse> *valve) {
        Header reqHeader;
        MetaRequest req;
        req.subject = subject;
        req.clientTypeCode = MetaRequest::ClientProducer;
        req.appCode = this->appCode;
        req.requestType = MetaRequest::ReqRegister;
        req.clientId = this->clientId;
        req.room = this->room;
        req.env = this->env;

        auto metaCallback = [this, valve, callback](int code, Header *respHeader, MetaResponse *resp) {
            if (code == MetaTcp::CodeSuccess)
            {
                resp->seconds = this->calibrator.seconds();
                MetaResponse *metaResp = new MetaResponse;
                *metaResp = *resp;
                valve->setTarget(metaResp);
            }
            else
            {
                if (callback)
                {
                    cout << "getPubMetaResp::MetaResponse_not_success" << endl;
                    callback(nullptr);
                }
            }
        };
        this->metaTcp.getTarget()->metaRequest(&reqHeader, &req, metaCallback);
    };
    valve.start(task, callback, 3000);
}

void Client::getBroker(MetaResponse *meta, function<void(BrokerTcp *)> callback)
{
    if (meta == nullptr)
    {
        cout << "getBroker::MetaResponse_is_nullptr" << endl;
        callback(nullptr);
        return;
    }
    // random broker info
    uint64_t now = uv_now(loop);
    size_t index = (size_t)(now % meta->brokers.size());
    BrokerInfo &info = meta->brokers[index];

    // RequestValve<BrokerTcp> tempValve(loop);
    auto pair = brokerMap.emplace(info.addr, RequestValve<BrokerTcp>(loop));
    RequestValve<BrokerTcp> &valve = pair.first->second;

    BrokerTcp *broker = valve.getTarget();
    if (broker != nullptr)
    {
        callback(broker);
        return;
    }
    // cout << info.toString() << endl;

    auto task = [this, &info, callback](RequestValve<BrokerTcp> *valve) {
        BrokerTcp *newBroker = new BrokerTcp(this->loop);
        newBroker->setInfo(info);
        cout << "setInfo:" << newBroker->getInfo().toString() << endl;
        newBroker->setConnectCallback([valve](BrokerTcp *tcp) {
            valve->setTarget(tcp);
        });
        newBroker->setTerminateCallback([valve](BrokerTcp *tcp, int code) {
            valve->clearTarget();
        });
        newBroker->setExile(30000);
        newBroker->connect();
    };

    valve.start(task, callback, 10000);
}

void Client::startHttp()
{
    if (reqInterval == 0)
    {
        if (error)
            error(0, "reqInterval");
        return;
    }
    uv_timer_init(loop, &httpTimer);
    httpTimer.data = this;
    http.setTimeout(1000);
    http.setCallback([this](int code, string &&body) {
        if (code == 200)
            this->handleHttpResp(forward<string>(body));
    });
    uv_timer_start(
        &httpTimer,
        [](uv_timer_t *handle) {
            Client *self = (Client *)handle->data;
            self->http.startGetRequest();
        },
        0, reqInterval);
}

void Client::startHeartbeat()
{
    uv_timer_init(loop, &heartbeatTimer);
    heartbeatTimer.data = this;
    uv_timer_start(
        &heartbeatTimer,
        [](uv_timer_t *t) {
            Client *self = (Client *)t->data;
            MetaTcp *tcp = self->metaTcp.getTarget();
            if (tcp != nullptr)
            {
                tcp->heartbeat();
            }
            for (auto iter = self->brokerMap.begin(); iter != self->brokerMap.end(); ++iter)
            {
                cout << "key:" << iter->first << endl;
                BrokerTcp *broker = iter->second.getTarget();
                if (broker != nullptr)
                {
                    cout << "Heartbeat:" << broker->getInfo().toString() << endl;
                    broker->heartbeat();
                }
            }
        },
        10000,
        10000);
}

void Client::startMetaTcp()
{
    cout << "startMetaTcp()" << endl;
    if (metaTcp.getTarget() != nullptr)
    {
        return;
    }

    auto task = [this](RequestValve<MetaTcp> *valve) {
        MetaTcp *newMetaTcp = new MetaTcp(loop);
        newMetaTcp->setAddr(move(this->ip), this->port);
        newMetaTcp->setConnectCallback([valve](MetaTcp *tcp) {
            valve->setTarget(tcp);
        });
        newMetaTcp->setTerminateCallback([this, valve](MetaTcp *tcp, int code) {
            if (valve->getTarget() == tcp)
            {
                valve->clearTarget();
                // 如果断开的MetaTcp不是Client引用的,说明是流放的连接,不需要管
                // 如果是Client引用的连接,则需要重启一个
                cout << "2.======>> startMetaTcp()" << endl;
                this->startMetaTcp();
            }
            else
            {
                delete tcp;
            }
        });
        newMetaTcp->setExile(30000);
        newMetaTcp->connect();
    };

    this->metaTcp.start(task, nullptr, 2000);
}

void Client::handleHttpResp(string &&resp)
{
    cout << "resp:" << resp << endl;
    try
    {
        const char *delim = ":";
        char *token = strtok((char *)resp.c_str(), delim);
        if (token == nullptr)
        {
            return;
        }
        string newIp(token);
        token = strtok(nullptr, delim);
        if (token == nullptr)
        {
            return;
        }
        int newPort(0);
        string strPort(token);
        newPort = stoi(strPort);

        // 如果ip或者port变更了,就使用新的ip和port新建metaTcp连接
        if ((ip != newIp) || (port != newPort))
        {
            cout << "oldIp:" << ip << " "
                 << "newIp:" << newIp << endl;
            cout << "oldPort:" << port << " "
                 << "newPort:" << newPort << endl;
            ip = newIp;
            port = newPort;
            cout << "1.======>> startMetaTcp()" << endl;
            startMetaTcp();
        }
    }
    catch (const exception &e)
    {
        return;
    }
}

string Client::genMsgId()
{
    int id = ++msgIdAcc;
    if (msgIdAcc == 999999)
    {
        msgIdAcc = 0;
    }
    stringstream stream;
    stream << calibrator.msgIdTime() << "#"
           << localIp << "#"
           << pid << "#"
           << id;
    return stream.str();
}

} // namespace Pitocin
