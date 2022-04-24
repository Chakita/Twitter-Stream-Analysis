from ast import arg
import tweepy
from tweepy import Stream
import socket
import json

with open('tokens.txt','r') as f:
    consumer_key=f.readline().strip()
    consumer_secret=f.readline().strip()
    access_key=f.readline().strip()
    access_secret=f.readline().strip()

class Listener(Stream):
    def __init__(self,csocket,*args):
        super().__init__(*args)
        self.client_socket=csocket;
        
    def on_data(self,data):
        try:
            message = json.loads(data)
            hashtags=['#VaccinationDrive','#fridayfeeling','#MondayMotivation','#traveltuesday','#vegan','#fitness']
            txt = message['text']
            hashtag_in_message=""
            for h in hashtags:
                if h in txt:
                    hashtag_in_message=h
                    if hashtag_in_message!="":
                        encode_message = hashtag_in_message.encode('utf-8')
                        print(encode_message)
                        self.client_socket.send(encode_message)
                        return True
                    else:
                        print("EMPTY")
        except:
            print("Error in trigger")
        return True
    def if_error(self,status):
        print(status)
        return True
def stream_tweets(csocket):
    # Twitter authentication
    hashtags=['#VaccinationDrive','#fridayfeeling','#MondayMotivation','#traveltuesday','#vegan','#fitness']
    tweet_stream = Listener(csocket,consumer_key, consumer_secret,access_key, access_secret)
    tweet_stream.filter(track=hashtags)

if __name__ == "__main__":
    new_socket = socket.socket()
    host = "127.0.0.1"
    port = 5555
    new_socket.bind((host,port))
    print("Listening on port:",port)
    new_socket.listen(5)
    c,address = new_socket.accept()
    print("Request from:",address)
    stream_tweets(c)
    
