#ifndef PITOCIN_IPV4_GETTER_H
#define PITOCIN_IPV4_GETTER_H

#include <arpa/inet.h>
#include <net/if.h>
#include <ifaddrs.h>
#include <string>
#include <cstring>
#include <regex>
#include <string>

using namespace std;

namespace Pitocin
{
class Ipv4Getter
{
    ifaddrs *addrs = nullptr;
    std::string ip;

public:
    Ipv4Getter()
    {
        regex re("[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}");
        char buf[20];
        int errCode = getifaddrs(&addrs);
        if (errCode != 0)
        {
            return;
        }
        for (ifaddrs *ifa = addrs; ifa != nullptr; ifa = ifa->ifa_next)
        {
            if (ifa->ifa_addr == NULL)
                continue;
            if (!(ifa->ifa_flags & IFF_UP))
                continue;
            if (ifa->ifa_addr->sa_family != AF_INET)
                continue;
            struct sockaddr_in *s4 = (struct sockaddr_in *)ifa->ifa_addr;
            void *inAddr = &s4->sin_addr;
            const char *data = inet_ntop(ifa->ifa_addr->sa_family, inAddr, buf, sizeof(buf));
            if (data == nullptr)
            {
                continue;
            }
            if (regex_match(data, re))
            {
                if (strcmp(data, "127.0.0.1") == 0)
                {
                    continue;
                }
                ip = string(data);
                return;
            }
        }
    }

    ~Ipv4Getter()
    {
        freeifaddrs(addrs);
    }

    std::string &toString()
    {
        return ip;
    }
};
} // namespace Pitocin

#endif //PITOCIN_IPV4_GETTER_H
