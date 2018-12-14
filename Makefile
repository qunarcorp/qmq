package:
	mvn -U clean package  -Pdist -Dmaven.test.skip=true -DskipTests -am -pl qmq-dist
