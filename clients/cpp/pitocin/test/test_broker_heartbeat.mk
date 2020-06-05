CFILE=	

HFILE=		../src/Conn.hpp \
			../src/StackPool.hpp \
			../src/Protocol.hpp \
			../src/BrokerTcp.hpp \
			../src/ExiledChecker.hpp \
			../src/RequestValve.hpp

test_conn: test_broker_heartbeat.cpp ${CFILE} ${HFILE}
	g++ -g -std=c++14 -I ../src -o test_broker_heartbeat test_broker_heartbeat.cpp  ${CFILE} -L /usr/lib/x86_64-linux-gnu -luv
