import multiprocessing
import time
import os

def main():
    iq = multiprocessing.JoinableQueue()
    oq = multiprocessing.Queue()
    plist = [multiprocessing.Process(target=worker, args=(iq, oq)) for i in range(2)]
    map(lambda x: x.start(), plist)
    data = range(100)
    for i in range(0, 100, 10):
        iq.put(data[i:i + 10])
    iq.join()
    for p in plist:
        try:
            p.terminate()
        except:
            pass
    sum_num = 0
    while not oq.empty():
        sum_num += oq.get()
    print 'sum', sum_num, sum(range(0, 100)) == sum_num

def worker(iq, oq):
    while 1:
        pid = os.getpid()
        data = iq.get()
        print 'start %s' % pid
        k = sum(data)
        print '%s do something %s' % (pid, k)
        print 'finish %s' % pid
        oq.put(k)
        iq.task_done()
        print '%s oq put %s' % (pid, k)
        

if __name__ == '__main__':
    main()
