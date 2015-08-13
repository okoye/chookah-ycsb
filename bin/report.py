'''
automatically parse throughput info for average latency
'''
import optparse

def parse(value, metric='avglatency'):
    '''
    supported metric values are:
        avglatency, throughput
    '''
    time, throughput, latency = -1, 0, -1
    try:
        time_raw, throughput_raw, latency_raw = value.split(';')
        time = int(time_raw.split()[0])
        throughput = float(throughput_raw.strip().split()[0])
        latency = float(latency_raw.split('READ AverageLatency(us)=')[1].replace("]", ""))
        latency = latency/1000
    except IndexError: #line has no ops
        pass
    except ValueError: #this is not READ result
        pass
    if metric == 'avglatency':
        return (time, latency)
    elif metric == 'throughput':
        return (time, throughput)

def dummy_emitter(start, end):
    for i in xrange(start, end):
        print "%s\t%s"%(i, -1)

def main(file_name, delta_time):
    delta_time = 10
    count = 0 - delta_time
    for line in open(file_name):
        count += delta_time
        if 'failed' in line:
            continue
        if 'current ops/sec' in line:
            time, metric = parse(line, metric='avglatency')
            if time != count:
                dummy_emitter(count, time)
            count = time
            print '%s\t%s'%(time, metric)

if __name__ == '__main__':
  parser = optparse.OptionParser()
  parser.add_option('-f', '--file', help='file name',
                    dest='f')
  parser.add_option('-d', '--delta', help='time step',
                    dest='d', default=10)
  (opts, args) = parser.parse_args()
  main(opts.f, int(opts.d))

