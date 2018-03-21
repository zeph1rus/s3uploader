#####
import sys,json,os,time,threading,Queue,logging
import random
import string
import boto
import math

def get_full_fpath(fname):
    try:
        config_object = json.load(open("config.json","r"))
        directory_location = config_object['monitor_location']
        return os.path.join(directory_location,fname)
    except:
        logging.error('could not read config')
        return None

def get_s3region():
    try:
        config_object = json.load(open("config.json","r"))
        return config_object['s3_region']
    except:
        logging.error('could not read config')
        return None


def directory_scan():
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


class s3Worker(threading.Thread):
    def __init__(self, threadID, name, q, bn):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
        self.bucket_name = bn
        #Files less than 5G can be processed in one upload
        #Since we expect to have files greater than 5G we need
        #to cater for them. 
        #this is now set to 250MB because i'm concerned about doing more than 250MB single part uploads
        self.max_s3_single_upload = 250 * 1024 * 1024
    def run(self):
        print ("Starting : "+ self.name)
        while 1:
            try:
                msg = self.q.get(True,10)
                if isinstance(msg, str) and msg == 'quit':
                    break
                else:
                    #assume we have a valid filename
                    try:
                        fsize = get_fsize(msg)
                    except Exception as e:
                        logger.error('Worker unable to stat file: {0} {1}'.format((str(msg),str(e))))
                        break
                    #assume now we have valid file we can upload
                    try:
                        print '{0} : Connecting to S3'.format(self.name)
                        s3 = boto.s3.connect_to_region(get_s3region())
                        logging.info('Connected to S3')
                        bucket = s3.get_bucket(self.bucket_name)
                    except Exception as e:
                        logging.error('Unable to get bucket s3 connection {0}'.format(str(e)))
                        break
                    try:
                        if fsize < self.max_s3_single_upload:
                            #do a single part upload to the s3 provider
                            print '{0} : single part upload'.format(self.name)
                            try:
                                key = boto.s3.key.Key(bucket,msg)
                                bytes_uploaded = key.set_contents_from_filename(get_full_fpath(msg))
                            except Exception as e:
                               logging.error('S3 Single Part Upload Failed : {0}'.format(str(e)))
                        else:
                            try:
                                print '{0} : multipart upload'.format(self.name)
                                key = boto.s3.key.Key(bucket,msg)
                                mp = bucket.initiate_multipart_upload(get_full_fpath(msg))
                                bytes_per_chunk = 250 * 1024 * 1024
                                chunk_count = int(math.ceil(fsize / float(bytes_per_chunk)))
                            except Exception as e:
                                logging.error('S3 Multipart Upload Failed in Initiation: {0} '.format(str(e)))
                                break
                            try:
                                for i in range(chunk_count):
                                    offset = i * bytes_per_chunk
                                    remaining_bytes = fsize - offset
                                    read_bytes = min([bytes_per_chunk,remaining_bytes])
                                    part = i + 1
                                    with open(get_full_fpath(msg), 'r') as file_ptr:
                                        file_ptr.seek(offset)
                                        print '{0} : Seeking to offset: {1}'.format(self.name, str(offset))
                                        mp.upload_part_from_file(fp = file_ptr, part_num = part, size = read_bytes)
                                if len(mp.get_all_parts()) == chunk_count:
                                    mp.complete_upload()
                                else:
                                    mp.cancel_upload()
                                    raise Exception('MP Upload cancelled due to failure of one or more parts to upload')
                            except Exception as e:
                                logging.error('{0} : S3 Multipart Upload failed during upload to provider: {1}'.format(self.name, str(e)))
                    except Exception as e:
                        logging.error('General error uploading {0}'.format(str(e)))
                        break
            except Queue.Empty:
                pass
            time.sleep(random.randint(0,9))
        print "Worker : {0} done".format(self.threadID)

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
            try:
                msg = self.manager_queue.get(True,3)
                if isinstance(msg, str) and msg == 'quit':
                    break
                else:
                    pass
            except Queue.Empty:
                logging.info("Manager Control Queue Empty")
            try:
                dir_contents = directory_scan()
                for z in dir_contents:
                    fsize = get_fsize(z)
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
            except Exception as e:
                logging.error("Manager error %s" % str(e))
            time.sleep(1)
        print "Manager Done"

if __name__ == '__main__':
    os.environ['S3_USE_SIGV4'] = 'True'
    worker_list = []
    manager_list = []
    worker_queue = Queue.Queue(0)
    manager_queue = Queue.Queue(0)
    try:
        config_object = json.load(open("config.json","r"))
        print "Current Config"
        print json.dumps(config_object)
        num_threads = config_object['num_threads']
        new_manager = s3Manager(worker_queue,manager_queue)
        new_manager.start()
        manager_list.append(new_manager)
        for tn in range(1, int(num_threads) + 1):
            new_worker = s3Worker(tn,"Worker" + str(tn),  worker_queue, config_object['bucket_name'])
            new_worker.start()
            worker_list.append(new_worker)
        while 1:
            time.sleep(2)
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

