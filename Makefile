

jar:
	mvn package

clean:
	mvn clean
	
upload:
	scp -i ~/.ssh/tacc-shared-key.pem target/index.jar ubuntu@129.114.16.34:/home/ubuntu/bin

