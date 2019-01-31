from kafka import KafkaProducer, KafkaConsumer
import time, sys
from time import gmtime, strftime


def main():
    producer = KafkaProducer(bootstrap_servers='ec2-52-88-251-94.us-west-2.compute.amazonaws.com:9092')
    
    file_address = "/home/ubuntu/input.txt"

    count = 0
    with open(file_address) as f:
        for line in f:
            curtime = strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            new_line = line 
            #new_line = line + "," + curtime
	    if new_line[0] != "":
                producer.send("ctest", new_line)
                count += 1
	    print("now it is sending... " + new_line)
    producer.flush()
    f.close()

if __name__ == "__main__":
    main()
