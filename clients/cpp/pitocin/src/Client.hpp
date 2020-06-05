#ifndef PITOCIN_CLIENT_H
#define PITOCIN_CLIENT_H

#include <uv.h>
#include <functional>
#include <string>
#include <map>
#include <utility>
#include <vector>
#include <unordered_map>

#include "Http.hpp"
#include "MetaTcp.hpp"
#include "BrokerTcp.hpp"
#include "Protocol.hpp"
#include "TimeCalibrator.hpp"
#include "RequestValve.hpp"

using namespace std;

namespace Pitocin
{
class Client
{
    struct PubCtx
    {
        Client *client;
        Header header;
        PublishMessage msg;
        BrokerTcp *broker = nullptr;
        function<void(string &, int)> callback;
    };

    uv_loop_t *loop;
    function<void(int, string &&)> error;
    bool running = false;
    string appCode;
    string clientId;
    string room;
    string env;
    // 生成msgId中时间的部分
    TimeCalibrator calibrator;
    int pid;
    string localIp;

    // meta http
    uv_timer_t httpTimer;
    uint64_t reqInterval = 30000;
    Http http;

    // meta tcp
    uv_timer_t heartbeatTimer;
    string ip = "";
    int port = 0;
    RequestValve<MetaTcp> metaTcp;
    unordered_map<string, RequestValve<MetaResponse>> metaRespMapPub;

    // broker
    unordered_map<string, RequestValve<BrokerTcp>> brokerMap;
    int msgIdAcc = 0;
    StackPool<PubCtx> pubCtxPool;

public:
    static const int CodeSuccess = 0;
    static const int CodeNoBroker = -1;
    static const int CodeTimeout = -2;
    static const int CodeError = -3;

public:
    Client() = delete;
    Client(uv_loop_t *loop);
    ~Client();
    void setBaseInfo(string &&_appCode, string &&_room, string &&_env);
    void setErrCallback(decltype(error) callback);
    void setHttpUrl(string &&url);
    void setHttpReqInterval(uint64_t interval);
    void start();
    void publish(
        const string &subject,
        const vector<pair<string, string>> &kvList,
        function<void(string &, int)> callback);

private:
    void startHttp();
    void startHeartbeat();
    void startMetaTcp();
    void httpRequest();
    void handleHttpResp(string &&resp);
    void getPubMetaResp(string &subject, function<void(MetaResponse *)> callback);
    void getBroker(MetaResponse *meta, function<void(BrokerTcp *)> callback);
    string genMsgId();
};
} // namespace Pitocin

#endif
