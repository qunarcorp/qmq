#ifndef PITOCIN_PROTOCOL_H
#define PITOCIN_PROTOCOL_H

#include <cstdint>
#include <string>
#include <sstream>
#include <vector>
#include <utility>
#include <cstring>

#include <iostream>

using namespace std;

namespace Pitocin
{

struct BigendReader
{
    uint8_t *buf = nullptr;
    size_t posit = 0;
    size_t size = 0;
    bool readError = false;

    BigendReader(bool error)
        : readError(error)
    {
    }
    BigendReader(uint8_t *p, size_t l)
        : buf(p),
          size(l)
    {
    }
    BigendReader &operator>>(uint8_t &n)
    {
        if (readError)
        {
            return *this;
        }
        if (size - posit < 1)
        {
            readError = true;
            return *this;
        }
        uint8_t *p = buf + posit;
        n = *p;
        posit += 1;
        return *this;
    }
    BigendReader &operator>>(uint16_t &n)
    {
        if (readError)
        {
            return *this;
        }
        if (size - posit < 2)
        {
            readError = true;
            return *this;
        }
        uint8_t *p = buf + posit;
        n = ((uint16_t)p[0] << 8) +
            (uint16_t)p[1];
        posit += 2;
        return *this;
    }
    BigendReader &operator>>(uint32_t &n)
    {
        if (readError)
        {
            return *this;
        }
        if (size - posit < 4)
        {
            readError = true;
            return *this;
        }
        uint8_t *p = buf + posit;
        n = ((uint32_t)p[0] << 24) +
            ((uint32_t)p[1] << 16) +
            ((uint32_t)p[2] << 8) +
            (uint32_t)p[3];
        posit += 4;
        return *this;
    }
    BigendReader &operator>>(uint64_t &n)
    {
        if (readError)
        {
            return *this;
        }
        if (size - posit < 8)
        {
            readError = true;
            return *this;
        }
        uint8_t *p = buf + posit;
        n = ((uint64_t)p[0] << 56) +
            ((uint64_t)p[1] << 48) +
            ((uint64_t)p[2] << 40) +
            ((uint64_t)p[3] << 32) +
            ((uint64_t)p[4] << 24) +
            ((uint64_t)p[5] << 16) +
            ((uint64_t)p[6] << 8) +
            (uint64_t)p[7];
        posit += 8;
        return *this;
    }

    BigendReader slice(size_t len)
    {
        if (readError)
        {
            return BigendReader(true);
        }
        if (size - posit < len)
        {
            readError = true;
            return BigendReader(true);
        }
        BigendReader reader(buf + posit, len);
        posit += len;
        return reader;
    }

    string readString(size_t len)
    {
        if (readError)
        {
            return "";
        }
        if (size - posit < len)
        {
            readError = true;
            return "";
        }
        string s((char *)(buf + posit), len);
        posit += len;
        return s;
    }
};

struct BigendStream
{
    uint8_t *buf = nullptr;
    size_t posit = 0;
    size_t capacity = 0;
    size_t size = 0;
    bool readError = false;

    BigendStream()
    {
    }
    BigendStream(size_t bufSize)
    {
        resize(bufSize);
    }
    ~BigendStream()
    {
        free(buf);
    }

    template <typename T>
    void insert(const T &data, size_t posit)
    {
        size_t oldSize = size;
        size = posit;
        *this << data;
        size = oldSize;
    }

    BigendStream &operator<<(uint8_t n)
    {
        checkSize(1);

        *(buf + size) = n;
        size += 1;
        return *this;
    }

    BigendStream &operator<<(uint16_t n)
    {
        checkSize(2);

        uint8_t *p = buf + size;
        p[0] = (n >> 8 & 0xff);
        p[1] = (n & 0xff);

        size += 2;
        return *this;
    }

    BigendStream &operator<<(uint32_t n)
    {
        checkSize(4);

        uint8_t *p = buf + size;
        p[0] = (n >> 24 & 0xff);
        p[1] = (n >> 16 & 0xff);
        p[2] = (n >> 8 & 0xff);
        p[3] = (n & 0xff);

        size += 4;
        return *this;
    }

    BigendStream &operator<<(uint64_t n)
    {
        checkSize(8);

        uint8_t *p = buf + size;
        p[0] = (n >> 56 & 0xff);
        p[1] = (n >> 48 & 0xff);
        p[2] = (n >> 40 & 0xff);
        p[3] = (n >> 32 & 0xff);
        p[4] = (n >> 24 & 0xff);
        p[5] = (n >> 16 & 0xff);
        p[6] = (n >> 8 & 0xff);
        p[7] = (n & 0xff);

        size += 8;
        return *this;
    }

    BigendStream &operator<<(const string &str)
    {
        size_t strLen = str.length();
        checkSize(strLen);

        uint8_t *p = buf + size;
        memcpy(p, str.data(), strLen);
        size += strLen;
        return *this;
    }

    void write(uint8_t *buffer, size_t len)
    {
        checkSize(len);

        uint8_t *p = buf + size;
        memcpy(p, buffer, len);
        size += len;
        return;
    }

    void compact()
    {
        if (posit == 0)
        {
            return;
        }
        size_t left = size - posit;
        if (left > 0)
        {
            memcpy(buf, buf + posit, left);
        }
        size -= posit;
        posit = 0;
    }

    void reset()
    {
        posit = 0;
        size = 0;
        readError = false;
    }

    BigendStream &operator>>(uint32_t &n)
    {
        if (readError)
        {
            return *this;
        }
        if (size - posit < 4)
        {
            readError = true;
            return *this;
        }
        uint8_t *p = buf + posit;
        n = ((uint32_t)p[0] << 24) +
            ((uint32_t)p[1] << 16) +
            ((uint32_t)p[2] << 8) +
            (uint32_t)p[3];
        posit += 4;
        return *this;
    }

    BigendStream &operator>>(uint16_t &n)
    {
        if (readError)
        {
            return *this;
        }
        if (size - posit < 2)
        {
            readError = true;
            return *this;
        }
        uint8_t *p = buf + posit;
        n = ((uint16_t)p[0] << 8) +
            (uint16_t)p[1];
        posit += 2;
        return *this;
    }

    BigendReader slice(size_t len)
    {
        if (readError)
        {
            return BigendReader(true);
        }
        if (size - posit < len)
        {
            readError = true;
            return BigendReader(true);
        }
        BigendReader reader(buf + posit, len);
        posit += len;
        return reader;
    }

    string toString()
    {
        stringstream s;

        s << "BigendStream [";
        if (size > 0)
        {
            size_t last = size - 1;
            for (size_t i = 0; i < last; ++i)
            {
                s << +buf[i] << ", ";
            }
            s << +buf[size - 1];
        }
        s << "]";
        return s.str();
    }

private:
    bool resize(size_t newSize)
    {
        if (newSize <= capacity)
        {
            return true;
        }
        uint8_t *newBuf = (uint8_t *)realloc(buf, newSize);
        if (newBuf)
        {
            buf = newBuf;
            capacity = newSize;
            return true;
        }
        return false;
    }
    void checkSize(size_t check)
    {
        if (size + check > capacity)
        {
            if (!resize((capacity + check) * 2))
            {
                throw bad_alloc();
            }
        }
    }
};

// header
struct Header
{
    /**
     * | name                | integer | remark                     
     * | :------------------ | :------ | :------------------------- 
     * | success             | 0       | 响应成功                   
     * | unknown_code        | 3       | 未知状态                   
     * | no_message          | 4       | 无消息(consumer拉消息)     
     * | send_message        | 10      | 发送消息                   
     * | pull_message        | 11      | 拉消息                     
     * | ack_request         | 12      | ack请求                    
     * | sync_request        | 20      | 同步请求                   
     * | broker_register     | 30      | 注册broker                 
     * | client_register     | 35      | 注册client                 
     * | broker_acquire_meta | 40      | 获取broker meta            
     * | broker_error        | 51      | broker异常(强制路由)       
     * | broker_reject       | 52      | broker拒绝此消息(强制路由) 
     * | heartbeat           | 100     | 心跳包                     
     * */
    uint16_t code;

    // 协议版本
    uint16_t version = 3;

    // 请求id,不断递增
    uint32_t requestId;

    /**
     * 消息标识
     * 0 - request
     * 1 - response
     * */
    uint32_t flag = 0;

    //
    uint16_t requestCode;

    static const uint16_t CodeSuccess = 0;
    static const uint16_t CodeSendMessage = 10;
    static const uint16_t CodeClientRegister = 35;
    static const uint16_t CodeHeartbeat = 100;

    static const uint32_t FlagRequest = 0;
    static const uint32_t FlagResponse = 1;

    static const uint32_t MagicNumber = 3737193182;

    void encode(BigendStream &stream)
    {
        // header len,固定长度
        stream << (uint16_t)18;

        // magic number
        stream << MagicNumber;

        //      code    version    request_id   flag    request_code
        stream << code << version << requestId << flag << requestCode;
    }

    bool decode(BigendReader &stream)
    {
        uint16_t headerLen;
        uint32_t magic;
        stream >> headerLen >> magic >> code >> version >> requestId >> flag >> requestCode;
        if (stream.readError)
        {
            return false;
        }
        if (headerLen != 18)
        {
            return false;
        }
        if (magic != MagicNumber)
        {
            return false;
        }
        return true;
    }

    string toString()
    {
        stringstream s;
        s << "Header {" << endl
          << "  code:" << +code << endl
          << "  version:" << +version << endl
          << "  requestId:" << +requestId << endl
          << "  flag:" << +flag << endl
          << "  requestCode:" << +requestCode << endl
          << "}";
        return s.str();
    }
};

// 请求meta的body结构
struct MetaRequest
{
    // 消息主题，必填
    string subject;

    /**
     * 必填
     * '1' - producer
     * '2' - consumer
     * '4' - delay_producer
     * */
    char clientTypeCode;

    // appCode 必填
    string appCode;

    /**
     * 请求类型，必填
     * '1' - register
     * '2' - heartbeat
     * 只有第一次的时候使用register
     * */
    char requestType;

    // 客户端唯一编号
    // 可以使用ip@appCode的形式,必填
    string clientId;

    // 机房，非必填
    string room;

    // 环境，非必填，如prod, uat, fat
    string env;

    static const char ClientProducer = '1';
    static const char ClientConsumer = '2';
    static const char ClientDelayProducer = '4';

    static const char ReqRegister = '1';
    static const char ReqHeartbeat = '2';

    void encode(BigendStream &stream)
    {
        size_t sizeMark = stream.size;
        // 5个必填项
        uint16_t count = 5;

        // body fields count 占位
        stream << (uint16_t)0;

        string subjectKey("subject");
        stream << (uint16_t)subjectKey.length() << subjectKey << (uint16_t)subject.length() << subject;
        string clientTypeCodeKey("clientTypeCode");
        stream << (uint16_t)clientTypeCodeKey.length() << clientTypeCodeKey << (uint16_t)1 << (uint8_t)clientTypeCode;
        string appCodeKey("appCode");
        stream << (uint16_t)appCodeKey.length() << appCodeKey << (uint16_t)appCode.length() << appCode;
        string requestTypeKey("requestType");
        stream << (uint16_t)requestTypeKey.length() << requestTypeKey << (uint16_t)1 << (uint8_t)requestType;
        string clientIdKey("consumerId");
        stream << (uint16_t)clientIdKey.length() << clientIdKey << (uint16_t)clientId.length() << clientId;
        if (room.length() > 0)
        {
            string roomKey("room");
            stream << (uint16_t)roomKey.length() << roomKey << (uint16_t)room.length() << room;
            count += 1;
        }
        if (env.length() > 0)
        {
            string envKey("env");
            stream << (uint16_t)envKey.length() << envKey << (uint16_t)env.length() << env;
            count += 1;
        }
        stream.insert(count, sizeMark);
    }
};

struct BrokerInfo
{
    // qmq server组名称，
    // 没一组的名称都不相同，
    // server的ip会发生变化，但是组名称不会变
    string brokerGroupName;
    // ip:port
    string addr;
    // ip地址
    string ip;
    // 端口
    int port;
    // 机房
    string room;
    // 更新时间戳
    uint64_t updateTimestamp;
    // 状态:
    // | 值   | 标识  | 含义       |
    // | :--- | :---- | :--------- |
    // | 1    | "rw"  | 可读可写． |
    // | 2    | "r"   | 只读       |
    // | 3    | "w"   | 只写       |
    // | 4    | "nrw" | 不可用     |
    uint8_t state;

    bool decode(BigendReader &stream)
    {
        uint16_t brokerGroupNameLen = 0;
        uint16_t addrLen = 0;
        uint16_t roomLen = 0;

        stream >> brokerGroupNameLen;
        brokerGroupName = stream.readString((size_t)brokerGroupNameLen);
        stream >> addrLen;
        addr = stream.readString((size_t)addrLen);
        if (!parseAddr(addr))
        {
            return false;
        }
        stream >> roomLen;
        room = stream.readString((size_t)roomLen);
        stream >> updateTimestamp;
        stream >> state;
        if (stream.readError)
        {
            return false;
        }
        return true;
    }

    string toString()
    {
        stringstream s;
        s << "BrokerInfo {"
          << " group:" << brokerGroupName
          << " ip:" << ip
          << " port:" << port
          << " room:" << room
          << " timestamp:" << +updateTimestamp
          << " state:" << +state
          << " }";
        return s.str();
    }

private:
    bool parseAddr(string addr)
    {
        char *cstr = (char *)addr.c_str();
        const char *delim = ":";
        char *cip = strtok(cstr, delim);
        if (cip == nullptr)
        {
            return false;
        }
        ip = string(cip);
        char *cport = strtok(nullptr, delim);
        if (cport == nullptr)
        {
            return false;
        }
        port = atoi(cport);
        return true;
    }
};

struct MetaResponse
{
    // 消息的主题，可以理解为topic
    string subject;
    // 和MetaRequest的clientTypeCode一致
    // remark: 请求的时候类型为char,值为'1','2','4'
    //         响应的时候类型为uint8_t,值为1,2,4
    //         这里是个坑，请注意！
    uint8_t clientTypeCode;
    // broker列表
    vector<BrokerInfo> brokers;

    // 附加字段,和协议无关
    // 获取到response的时间,用来判断是否需要刷新
    uint64_t seconds;

    bool decode(BigendReader &stream)
    {
        uint16_t subjectLen;
        uint16_t count = 0;

        stream >> subjectLen;
        subject = stream.readString(subjectLen);
        stream >> clientTypeCode >> count;
        if (stream.readError)
        {
            return false;
        }
        brokers.clear();
        for (uint16_t i = 0; i < count; ++i)
        {
            BrokerInfo broker;
            if (broker.decode(stream))
            {
                brokers.push_back(broker);
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    string toString()
    {
        stringstream s;
        s << "MetaResponse {" << endl
          << "  subject:" << subject << endl
          << "  clientTypeCode:" << +clientTypeCode << endl
          << "  brokers:[" << endl;
        for (auto b : brokers)
        {
            s << "    " << b.toString() << endl;
        }
        s << "  ]" << endl
          << "}";
        return s.str();
    }
};

struct PublishMessage
{
    // 消息可靠性
    uint8_t flag;
    // 创建时间戳
    uint64_t beginTs;
    // 过期时间戳,0表示永不过期
    uint64_t endTs = 0;
    // 消息主题
    string subject;
    // 消息id
    string id;
    // kv列表
    vector<pair<string, string>> kvList;

    static const uint8_t FlagHigh = 0;
    static const uint8_t FlagLow = 1;

    void encode(BigendStream &stream)
    {
        stream << flag << beginTs << endTs
               << (uint16_t)subject.length() << subject
               << (uint16_t)id.length() << id;
        size_t sizeMark = stream.size;
        stream << (uint32_t)0;
        for (auto kv : kvList)
        {
            stream << (uint16_t)kv.first.length() << kv.first
                   << (uint16_t)kv.second.length() << kv.second;
        }
        uint32_t kvSize = stream.size - sizeMark - 4;
        stream.insert(kvSize, sizeMark);
    }
};

struct PublishMessageResult
{
    string id;
    uint32_t code;
    string remark;

    // (0) -> success;
    // (1) -> busy;
    // (2) -> duplicated;
    // (3) -> unregistered;
    // (4) -> slave;
    // (5) -> intercepted;
    // (6) -> unstored.
    static const uint32_t CodeParseError = 65536;
    static const uint32_t CodeSuccess = 0;
    static const uint32_t CodeBusy = 1;
    static const uint32_t CodeDuplicated = 2;
    static const uint32_t CodeUnregistered = 3;
    static const uint32_t CodeSlave = 4;
    static const uint32_t CodeIntercepted = 5;
    static const uint32_t CodeUnstored = 6;

    /*
        << IdLen:16/big
        , Id:IdLen/binary
        , Code:32/big
        , RemarkLen:16/big
        , Remark:RemarkLen/binary >>
    */
    bool decode(BigendReader &stream)
    {
        uint16_t idLen;
        uint16_t remarkLen;
        stream >> idLen;
        id = stream.readString(idLen);
        stream >> code;
        stream >> remarkLen;
        remark = stream.readString(remarkLen);
        if (stream.readError)
        {
            return false;
        }
        else
        {
            return true;
        }
    }
};

} // namespace Pitocin

#endif
