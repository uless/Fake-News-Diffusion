import tweepy
import pandas
import jsonpickle
from tweepy.streaming import *
import time

consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

maxTweets = 1000000
tweetsPerQry = 100

class MyStreamListener(tweepy.StreamListener):

    def __init__(self):
        tweepy.StreamListener.__init__(self)
        
        self.stream = None

    #Overload the on_status method
    def on_status(self, status):
        try:
            print("Got tweet with tweet id: " + str(status._json["id"]))
            with open("tweets.txt", 'a') as outfile:
                outfile.write(jsonpickle.encode(status._json) + "\n")
            hashtags_to_add = []
            for hashtag in status._json["entities"]["hashtags"]:
                to_add = True
                for each_hashtag in self.stream.search_terms:
                    if each_hashtag[1:].lower() == hashtag["text"].lower():
                        # print(each_hashtag[1:].lower(), hashtag["text"].lower())
                        to_add = False
                        break
                if to_add:
                    hashtags_to_add.append("#" + hashtag["text"])
            if len(hashtags_to_add) > 0:
                self.stream.search_terms += hashtags_to_add
                self.stream.change_filter = True
            if not status._json["user"]["id"] in self.stream.user_ids:
                self.stream.user_ids.append(status._json["user"]["id"])
            if "retweeted_status" in status._json.keys() and (not status._json["user"]["id"] in self.stream.user_ids):
                self.stream.user_ids.append(status._json["retweeted_status"]["user"]["id"])
            print("Done processing tweet with tweet id: " + str(status._json["id"]))
        #Error handling
        except BaseException as e:
            print("Error on_status: %s" % str(e))
            
        return True
 
    #Error handling
    def on_error(self, status):
        print("Listener got an error: ", end=',')
        print(status)
        return True

    #Timeout handling
    def on_timeout(self):
        return True

class MyStream(tweepy.Stream):

    def __init__(self, auth, listener, api, **options):
        tweepy.Stream.__init__(self, auth=auth, listener=listener, options=options)
        self.change_filter = False
        self.search_terms = []
        self.user_ids = []
        self.curr_user_id = 0
        self.api = api

    def setSearchTerms(self):
        search_file = open("hashtags.txt", 'r')
        self.search_terms = search_file.read().split("\n")

    def _read_loop(self, resp):
        buf = ReadBuffer(resp.raw, self.chunk_size)

        while self.running and not resp.raw.closed and (not self.change_filter):
            length = 0
            while not resp.raw.closed:
                line = buf.read_line().strip()
                if not line:
                    self.listener.keep_alive()  # keep-alive new lines are expected
                elif line.isdigit():
                    length = int(line)
                    break
                else:
                    raise TweepError('Expecting length, unexpected value found')

            next_status_obj = buf.read_len(length)
            if self.running:
                self._data(next_status_obj)

            # # Note: keep-alive newlines might be inserted before each length value.
            # # read until we get a digit...
            # c = b'\n'
            # for c in resp.iter_content(decode_unicode=True):
            #     if c == b'\n':
            #         continue
            #     break
            #
            # delimited_string = c
            #
            # # read rest of delimiter length..
            # d = b''
            # for d in resp.iter_content(decode_unicode=True):
            #     if d != b'\n':
            #         delimited_string += d
            #         continue
            #     break
            #
            # # read the next twitter status object
            # if delimited_string.decode('utf-8').strip().isdigit():
            #     status_id = int(delimited_string)
            #     next_status_obj = resp.raw.read(status_id)
            #     if self.running:
            #         self._data(next_status_obj.decode('utf-8'))


        if resp.raw.closed:
            self.on_closed(resp)

    def getUser(self, user_id):
        print("Getting tweets for user id: " + str(user_id))
        max_count = 3200
        curr_count = 0
        while curr_count < max_count:
            with open(str(user_id)+".txt", 'a') as outfile:
                res = self.api.user_timeline(user_id, count=200, page=(curr_count/200) + 1)
                curr_count += len(res)
                for status in res:
                    outfile.write(jsonpickle.encode(status._json) + "\n")
        print("Done getting tweets for user id: " + str(user_id))


    def _run(self):
        # Authenticate
        url = "https://%s%s" % (self.host, self.url)

        # Connect and process the stream
        error_counter = 0
        resp = None
        exception = None
        try:
            with open("current_hashtags.txt", 'w') as outfile:
                outfile.write(str(self.body))
        except e:
            pass
        while self.running and (not self.change_filter):
            if self.retry_count is not None:
                if error_counter > self.retry_count:
                    # quit if error count greater than retry count
                    break
            try:
                auth = self.auth.apply_auth()
                resp = self.session.request('POST',
                                            url,
                                            data=self.body,
                                            timeout=self.timeout,
                                            stream=True,
                                            auth=auth,
                                            verify=self.verify)
                if resp.status_code != 200:
                    if self.listener.on_error(resp.status_code) is False:
                        break
                    error_counter += 1
                    if resp.status_code == 420:
                        start_time = time.time()
                        for i in range(self.curr_user_id, len(self.user_ids)):
                            with open("user_ids_collected.txt", 'a') as user_file:
                                user_file.write(str(self.user_ids[i]) + "\n")
                            self.getUser(self.user_ids[i])
                        self.curr_user_id = len(self.user_ids)
                        end_time = time.time()
                        self.retry_time = max(self.retry_420_start,
                                              self.retry_time)
                        if (end_time - start_time) < self.retry_time:
                            print("Time left, going to sleep for: " + str(self.retry_time - (end_time - start_time)))
                            sleep(self.retry_time - (end_time - start_time))
                            print("Back from sleep")
                    #self.retry_time = min(self.retry_time * 2, self.retry_time_cap)
                else:
                    error_counter = 0
                    self.retry_time = self.retry_time_start
                    self.snooze_time = self.snooze_time_step
                    self.listener.on_connect()
                    self._read_loop(resp)
            except (Timeout, ssl.SSLError) as exc:
                # This is still necessary, as a SSLError can actually be
                # thrown when using Requests
                # If it's not time out treat it like any other exception
                if isinstance(exc, ssl.SSLError):
                    if not (exc.args and 'timed out' in str(exc.args[0])):
                        exception = exc
                        break
                if self.listener.on_timeout() is False:
                    break
                if self.running is False:
                    break
                sleep(self.snooze_time)
                self.snooze_time = min(self.snooze_time + self.snooze_time_step,
                                       self.snooze_time_cap)
            except Exception as exc:
                exception = exc
                # any other exception is fatal, so kill loop
                break

        # cleanup
        self.running = False
        self.change_filter = False
        if resp:
            resp.close()

        self.new_session()
        self.filter(track=self.search_terms)

        if exception:
            # call a handler first so that the exception can be logged.
            self.listener.on_exception(exception)
            raise exception


myStreamListener = MyStreamListener()
myStream = MyStream(auth = api.auth, listener=myStreamListener, api=api)
myStream.setSearchTerms()
myStreamListener.stream = myStream

myStream.filter(track=myStream.search_terms)
