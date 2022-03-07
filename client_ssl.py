import multiprocessing
import socket
import sys
import time
import ssl

try:
    import argparse
except:
    # argparse is only available in Python 2.7+
    print >> sys.stderr, 'pip install -U argparse'
    sys.exit(1)


TIMEOUT = 30  # 30 seconds operation timeout
FAILED = -1


def send_request(request_id):
    HOST, PORT = '127.0.0.1', 8000

    start = time.time()
    socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket_ = ssl.wrap_socket(socket_, cert_reqs=ssl.CERT_NONE)
    socket_.settimeout(TIMEOUT)
    try:
        data = str(time.time())

        socket_.connect((HOST, PORT))
        socket_.sendall(data)
        reply = socket_.recv(32)

        if reply != data:
            raise Exception('data: %s, reply: %s' % (data, reply))

        finish = time.time()
        response_time = finish - start

        return response_time
    except Exception:
        return FAILED
    finally:
        socket_.close()


def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('requests', type=int,
                           help='Total number of requests')
    argparser.add_argument('--workers', type=int, default=1,
                           help=('Number of workers to generate requests in '
                                 'parallel'))
    args = argparser.parse_args()
    workers = multiprocessing.Pool(args.workers)

    start = time.time()
    results = []
    for i in xrange(args.requests):
        workers.apply_async(send_request, args=(i,), callback=results.append)
    workers.close()
    workers.join()
    finish = time.time()

    succeeds = [i for i in results if i >= 0]
    errors = [i for i in results if i < 0]
    overall = sum(succeeds)
    if succeeds:
        avg = overall / len(succeeds) * 1000  # in ms.
    else:
        avg = 0
    time_spent = finish - start
    rps = args.requests / time_spent

    msg = ('Errors: %s, Succeeds: %s\n'
           'Response time (avg.): %s ms\n'
           'Requests per second (avg.): %s req/s\n'
           'Time spent: %s s')
    msg = msg % (len(errors), len(succeeds), avg, rps, time_spent)
    print msg


if __name__ == '__main__':
    main()
