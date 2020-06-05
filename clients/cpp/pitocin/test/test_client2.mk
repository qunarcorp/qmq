CLIENT_C=	../src/Client.cpp

CLIENT_H=	../src/Client.hpp \
			../src/Http.hpp \
			../src/Conn.hpp \
			../src/StackPool.hpp \
			../src/Protocol.hpp \
			../src/BrokerTcp.hpp \
			../src/ExiledChecker.hpp \
			../src/Ipv4Getter.hpp \
			../src/MetaTcp.hpp \
			../src/RequestValve.hpp \
			../src/TimeCalibrator.hpp

test_conn: test_client2.cpp ${CLIENT_C} ${CLIENT_H}
	g++ -g -std=c++14 -I ../src -o test_client2 test_client2.cpp  ${CLIENT_C} -L /usr/lib/x86_64-linux-gnu -luv -lhttp_parser
