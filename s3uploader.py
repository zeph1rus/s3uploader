#####
import sys,json,os,time,threading,Queue,logging
import random
import string
import boto

def get_full_fpath(fname):
    try:
        config_object = json.load(open("config.json","r"))
        directory_location = config_object['monitor_location']
        return os.path.join(directory_location,fname)
    except:
        logging.error('could not read config');
        return None

def directory_scan():
    print "Directory Scan"
    try:
        config_object = json.load(open("config.json","r"))
        directory_location = config_object['monitor_location']
        filetype_filter = config_object['filetype_filter']
    except:
        logging.error('Could not read config')
        return None
    try:
        file_list = [ x for x in os.listdir(directory_location) if
                     x.endswith(filetype_filter)]
    except:
        logging.error('Could not stat directory')
        return None
    return file_list

def get_fsize(fpath):
    try:
        config_object = json.load(open("config.json","r"))
        directory_location = config_object['monitor_location']
        fsize = os.stat(os.path.join(directory_location,fpath)).st_size
        return fsize
    except:
        logging.error('Could not stat file: %s ' % fpath)
        return None

class s3Deleter
    def __init__(self, threadID, name, q)
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
    def run(self):
        logging.info('starting deletion worker')
        while 1:
            msg = self.q.get(True,10)
            if isinstance(msg, str) and msg == 'quit':
                break
            else:
                try:
                    os.remove(get_full_fpath(msg))
                except Exception as e:
                    logging.error('Worker unable to delete file: %s %s' %
                                  (msg,str(e)))



class s3Worker(threading.Thread):
    def __init__(self, threadID, name, q, dq, bn):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
        self.dq = dq
        self.bucket_name = bn
        #Files less than 5G can be processed in one upload
        #Since we expect to have files greater than 5G we need
        #to cater for them. 
        self.max_s3_single_upload = 4988880000
    def run(self):
        print ("Starting: "+ self.name)
        while 1:
            msg = self.q.get(True,10)
            if isinstance(msg, str) and msg == 'quit':
                break
            else:
                #assume we have a valid filename
                try:
                    fsize = get_fsize(msg)
                except Exception as e:
                    logger.error('Worker unable to stat file: %s %s' %
                                 (str(msg),str(e)))
                    break
                #assume now we have valid file we can upload
                try:
                    s3 = boto.s3.connect_to_region('eu-west-2')
                    logging.info('Connected to S3')
                    bucket = s3.get_bucket(self.bucket_name)
                    logging.info('Acquired bucket')
                except Exception as e:
                    logging.error('Unable to get bucket s3 connection %s' %
                                 str(e))
                    break
                try:
                    if fsize < self.max_s3_single_upload:
                        #do a single part upload to the s3 provider
                        try:
                            key = boto.s3.key.Key(bucket,msg)
                            bytes_uploaded = key.set_contents_from_filename(get_full_fpath(msg))
                        except Exception as e:
                            logging.error('S3 Single Part Upload Failed: %s' %
                                          str(e))
                    else:
                        try:
                            key = boto.s3.key.Key(bucket,msg)
                            mp = bucket.initiate_multipart_upload(get_full_fpath(msg))
                            bytes_per_chunk = 5000*1024*1024
                            chunk_count = int(math.ceil(fsize /
                                                        float(bytes_per_chunk)))
                        except Exception as e:
                            logging.error('S3 Multipart Upload failed in
                                          initiation: %s' % str(e))
                            break
                        try:
                            for i in range(chunk_count):
                                offset = i * bytes_per_chunk
                                remaining_bytes = fsize - offset
                                read_bytes =
                                min([bytes_per_chunk,remaining_bytes])
                                part = i + 1
                                with open(get_full_fpath(msg),'r') as file_ptr:
                                    file_ptr.seek(offset)
                                    mp.upload_part_from_file(fp = file_ptr,
                                                             part_num = part,
                                                             size = read_bytes)
                            if len(mp.get_all_parts()) == chunk_count:
                                mp.complete_upload()
                            else:
                                mp.cancel_upload()
                                raise Exception('MP upload cancelled due to
                                                failure of one or more parts to
                                                upload')
                        except Exception as e:
                            logging.error('S3 Multipart Upload failed during upload
                                      to provider: %s' % str(e))
                except Exception as e:
                    #TODO: log failure
                    break

            print "Thread Message Received: %s , Thread: %s" % (msg,self.name)
            time.sleep(random.randint(0,9))
        print "Thread %i done" % self.threadID

class s3Manager(threading.Thread):
    def __init__(self, wq, mq):
        threading.Thread.__init__(self)
        self.work_queue = wq
        self.manager_queue = mq
        self.working_files_upload = []
        self.working_files_monitor = {}
    def run(self):
        print("Starting Manager Queue")
        while 1:
            print "manager loop"
            try:
                msg = self.manager_queue.get(True,3)
                if isinstance(msg, str) and msg == 'quit':
                    break
                else:
                    print "no quite received"
                print "Thread Message Received: %s , Thread: Manager" % (msg)
            except Queue.Empty:
                logging.info("Manager Control Queue Emptry")
            try:
                dir_contents = directory_scan()
                for z in dir_contents:
                    fsize = get_fsize(z)
                    print z
                    if not z in self.working_files_upload:
                        if not z in self.working_files_monitor:
                            self.working_files_monitor[z] = fsize
                        else:
                            if self.working_files_monitor[z] < fsize:
                                self.working_files_monitor[z] = fsize
                            else:
                                self.working_files_upload.append(z)
                                del self.working_files_monitor[z]
                                self.work_queue.put(z)
                    else:
                        logging.info('File: %s already in upload list' % z)
                    print fsize
                    print self.working_files_monitor
                    print self.working_files_upload
            except Exception as e:
                logging.error('Manager error:' + str(e))
            print "sleeping"
            time.sleep(1)
        print "Manager Done"

if __name__ == '__main__':
    os.environ['S3_USE_SIGV4'] = 'True'
    worker_list = []
    manager_list = []
    deleter_list = []
    worker_queue = Queue.Queue(0)
    manager_queue = Queue.Queue(0)
    deleter_queue = Queue.Queue(0)
    try:
        config_object = json.load(open("config.json","r"))
        print "Current Config"
        print json.dumps(config_object)
        num_threads = config_object['num_threads']
        print num_threads
        new_manager = s3Manager(worker_queue,manager_queue)
        new_manager.start()
        manager_list.append(new_manager)
        new_deleter = s3Deleter(1,'Deleter',deleter_queue)
        new_deleter.start()
        deleter_list.append(new_deleter)
        for tn in range(1, int(num_threads) + 1):
            new_worker = s3Worker(tn,"Worker" + str(tn),
                                  worker_queue,deleter_queue,config_object['bucket_name'])
            print "worker created"
            new_worker.start()
            worker_list.append(new_worker)
        while 1:
            time.sleep(2)
            randstr = ''.join(random.choice(string.ascii_uppercase + string.digits)
                              for _ in range(10))
            #worker_queue.put(randstr)
    except KeyboardInterrupt:
        print "Exiting due to keyboard input (CTRL-C)"
        manager_queue.put('quit')
        for tn in worker_list:
            worker_queue.put('quit')
        time.sleep(10)
        for tn in worker_list:
            tn.join()
        for mn in manager_list:
            mn.join()
        raise SystemExit
    except Exception as e:
        logging.error(str(e))
        raise SystemExit

