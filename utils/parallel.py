'多线程 与 多进程'
import sys
import ctypes
import time
from collections import deque
import threading
import multiprocessing
import threadpool

def my_print(content, file=sys.stdout, model=0):
    ''' 打印函数
        File    文件名, str, model=1或2时需要填写
        model   输出模式, int,
                model=0, 仅输出到缓冲区
                model=1, 仅输出到文件
                model=2, 同时输出到缓冲区和文件
        '''
    if model == 0:
        print(content)
    elif model == 1:
        out = open(file, 'a+')
        print(content, file=out)
        out.close()
    elif model == 2:
        out = open(file, 'a+')
        print(content)
        print(content, file=out)
        out.close()

def func_io(para, model=0, max_sleep_time=10):
    'test io func'
    import random
    file_name = 'temp.io.txt'
    my_print('%s, 开始运行' % (para), file_name, model)
    sleep_time = random.randint(0, max_sleep_time)
    time.sleep(sleep_time)
    my_print('%s, 运行结束, 休眠%d秒' % (para, sleep_time), file_name, model)

def func_cpu(para, model=0, some=0):
    'test cpu func'
    file_name = 'temp.cpu.txt'
    #my_print('%s, 开始运行' % (para), file_name, model)
    num = para * para + some
    my_print('%s, 运行结束, 结果为%d' % (para, num), file_name, model)
    return num

def run_thread(req_list, is_lock=True, limit_num=32):
    ''' 多线程函数
        req_list    任务列表, list, 每个元素为一个任务, 形式为
                    [
                        (func_0, (para_0_1, para_0_2, *,), [name_0]),
                        (func_1, (para_1_1, para_1_2, *,), [name_1]),
                        *
                    ]
        limit_num   最大线程数, int, 默认为8
        '''
    queue = deque(req_list)
    threads = []
    while len(queue):
        if threading.active_count() <= limit_num:
            para = queue.popleft()
            thread_name = para[2] if len(para) > 2 else None
            now_thread = threading.Thread(target=para[0], args=para[1], name=thread_name, daemon=True)
            now_thread.start()
            threads.append(now_thread)
    if is_lock:
        for now_thread in threads:
            # if now_thread is not threading.currentThread():
            now_thread.join()

def run_process(req_list, is_lock=True, limit_num=8):
    ''' 多进程函数
        req_list    任务列表, list, 每个元素为一个任务, 形式为
                    [
                        (func_0, (para_0_1, para_0_2, *,), [name_0]),
                        (func_1, (para_1_1, para_1_2, *,), [name_1]),
                        *
                    ]
        limit_num   最大线程数, int, 默认为8
        '''
    queue = deque(req_list)
    processes = []
    while len(queue):
        if len(multiprocessing.active_children()) <= limit_num:
            para = queue.popleft()
            process_name = para[2] if len(para) > 2 else None
            now_process = multiprocessing.Process(target=para[0], args=para[1], name=process_name)
            now_process.start()
            processes.append(now_process)
    if is_lock:
        for now_process in processes:
            now_process.join()

def run_process_pool(req_list, is_lock=True, limit_num=8):
    ''' 多进程程函数, 使用进程池
        req_list    任务列表, list, 每个元素为一个任务, 形式为
                    [
                        (func_0, (para_0_1, para_0_2, *,)),
                        (func_1, (para_1_1, para_1_2, *,)),
                        *
                        ]
        if_debug    debug模式开关, bool, 默认为False, 为True时会显示运行时间
        limit_num   最大进程数, int, 默认为8
        '''
    pool = multiprocessing.Pool(limit_num)
    # queue = deque(req_list)
    # while len(queue):
    #     para = queue.popleft()
    for func, params in req_list:
        pool.apply_async(func, args=params)
    if is_lock:
        pool.close()
        pool.join()

def run_thread_pool(req_list, is_lock=True, limit_num=8):
    ''' 多进程程函数, 使用进程池
        req_list    任务列表, list, 每个元素为一个任务, 形式为
                    [
                        (func_0, (para_0_1, para_0_2, *,)),
                        (func_1, (para_1_1, para_1_2, *,)),
                        *
                        ]
        if_debug    debug模式开关, bool, 默认为False, 为True时会显示运行时间
        limit_num   最大进程数, int, 默认为8
        '''
    pool = threadpool.ThreadPool(limit_num)
    _ = [[pool.putRequest(req)
          for req in threadpool.makeRequests(each[0], [(list(each[1]), None)])]
         for each in req_list]
    if is_lock:
        pool.wait()

def search_thread(name, part=False):
    '返回是否存在名为name的线程'
    thread_list = [thread.getName() for thread in threading.enumerate()]
    if not part:
        return name in thread_list
    else:
        for each in thread_list:
            if name in each:
                break
        else:
            return False
        return True

def main():
    '主函数'
    req_list = [(func_cpu, (num, 1, 0, )) for num in range(100)]
    # print(run_thread(req_list))
    # reply = {
    #     0:run_thread(req_list),
    #     1:run_process_pool(req_list),
    #     2:run_thread_pool(req_list)
    # }
    # if len(sys.argv) > 1:
    #     argv1 = int(sys.argv[1])
    #     if argv1 in reply.keys():
    #         reply[argv1]
    #pause(10)
    run_process_pool(req_list, True)

def profile():
    '性能分析'
    import cProfile
    cProfile.run('main()', 'profile.vpt')

def time_it(num=5):
    '测试程序用时'
    import timeit
    print(timeit.timeit('main()', 'from __main__ import main', number=num) / num)

def pause(_time=10):
    '暂停一段时间, 防止程序运行过快出错'
    print('wait %d sec' % (int(_time)))
    time.sleep(int(_time))

def kill_thread(thread):
    thread._reset_internal_locks(False)
    thread_id = ctypes.c_long(thread._ident)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, ctypes.py_object(SystemExit)) 
    if res > 1: 
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread_id, 0) 
        print('Exception raise failure') 

def kill_this_thread():
    kill_thread(threading.current_thread())

if __name__ == '__main__':
    time_it(1)
