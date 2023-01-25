import re


def get_stats(path):
    sum_ = dict(write=0, read=0)
    count_ = dict(write=0, read=0)
    max_ = dict(write=0, read=0)
    min_ = dict(write=99999999, read=9999999999)

    read_duration_list = []
    write_duration_list = []

    with open(path, 'r') as f:
        for line in f.readlines():
            write_log = re.search(fr'write.*microseconds: (.*)', line)
            if write_log:
                microsecs = int(float(write_log.group(1)))
                write_duration_list.append(microsecs)
                sum_['write'] += microsecs
                count_['write'] += 1
                max_['write'] = max(max_['write'], microsecs)
                min_['write'] = min(min_['write'], microsecs)
                continue
            read_log = re.search(fr'read.*microseconds: (.*)', line)
            if read_log:
                microsecs = int(float(read_log.group(1)))
                read_duration_list.append(microsecs)
                sum_['read'] += microsecs
                count_['read'] += 1
                max_['read'] = max(max_['read'], microsecs)
                min_['read'] = min(min_['read'], microsecs)

    write_duration_list.sort()
    read_duration_list.sort()

    print(path)
    print('sum', sum_)
    print('count', count_)
    print('max', max_)
    print('min', min_)
    print('average read: milisecs', sum_['read'] / count_['read'] / 1000)
    print('average write: milisecs', sum_['write'] / count_['write'] / 1000)
    print('read_duration_list median', read_duration_list[len(read_duration_list) // 2] / 1000)
    print('write_duration_list median', write_duration_list[len(write_duration_list) // 2] / 1000)
    print('')


if __name__ == '__main__':
    # this script is for development purpose only
    par_dir = '/Users/aayush/Documents/tbc masters/disseration/mastersthesis-webcrawler/data/app logs'
    path1 = f'{par_dir}/crawler_2023-01-22_07h24m38s_UTC.log.redis'
    path2 = f'{par_dir}/crawler_2023-01-22_10h18m11s_UTC.log.mongodb'
    path3 = f'{par_dir}/crawler_2023-01-22_10h04m10s_UTC.log.scylladb'
    path4 = f'/tmp/crawler_2023-01-22_11h00m28s_UTC.log'

    get_stats(path1)
    get_stats(path2)
    get_stats(path3)
    # get_stats(path4)
