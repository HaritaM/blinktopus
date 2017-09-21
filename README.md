# Blinktopus

How to start the Blinktopus application
---

1. Run `mvn package` to build your application
1. Start application with `java -jar target/blinktopus-0.0.1-SNAPSHOT.jar server config.yml`
1. To check that your application is running enter url `http://localhost:8080/log`
1. All entries in log: `http://localhost:8080/log`
1. All SVs: `http://localhost:8080/sv`
1. Query (create new SV): `http://localhost:8080/query?SVid=&type=row&table=orders&attr=totalPrice&lower=100&higher=5000&create=true`
1. Query (use old SV): `http://localhost:8080/query?SVid=row1&type=row&table=orders&attr=totalPrice&lower=1000&higher=3000&create=false`

Health Check
---

To see your applications health enter url `http://localhost:8081/healthcheck`
