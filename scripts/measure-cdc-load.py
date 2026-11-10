"""Run bounded CDC concurrency and sustained-write comparisons on the ECS host.

Uses uniquely tagged departments, verifies every successful update in the archive,
and removes owned business rows on exit. Requires the latency benchmark dependencies
and kafka-python. Resource samples use Linux cgroup v2 and existing Docker containers.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import importlib.util
import json
import math
from pathlib import Path
import re
import statistics
import struct
import subprocess
import threading
import time
import uuid

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient
import redis
import requests


parser = argparse.ArgumentParser()
parser.add_argument('--project', required=True)
parser.add_argument('--base-url', default='http://127.0.0.1:18081')
parser.add_argument('--kafka-bootstrap', default='127.0.0.1:9092')
parser.add_argument('--label', required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--workers', type=int, nargs='+', default=[1, 5, 10])
parser.add_argument('--iterations', type=int, default=30)
parser.add_argument('--warmup', type=int, default=5)
parser.add_argument('--write-seconds', type=float, default=15)
parser.add_argument('--observe-kafka', action='store_true', help='Correlate unique versions with an independent Kafka observer')
parser.add_argument('--skip-write-burst', action='store_true')
parser.add_argument('--observe-jvm', action='store_true', help='Correlate existing HotSpot perfdata GC pause counters')
args = parser.parse_args()
project = Path(args.project)
spec = importlib.util.spec_from_file_location('acceptance_config', project / 'scripts/stockout-acceptance.py')
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
cfg = helper.configuration()
base = args.base_url.rstrip('/')
tag = 'cdc-load-' + uuid.uuid4().hex[:8]
started = time.perf_counter()
report = {'started_at': datetime.now(timezone.utc).isoformat(), 'label': args.label,
          'run_id': tag, 'completed': False, 'phases': [], 'resource_samples': [],
          'method': 'Closed-loop cache-warm/PUT/invalidation/GET per worker; separate timed PUT-only burst.',
          'probe_pause_seconds': 0.005, 'resource_sample_seconds': 1,
          'arguments': vars(args)}
owned = []
versions = {}
expected = set()
archived_codes = set()
active = {}
kafka_seen = {}
kafka_condition = threading.Condition()
active_lock = threading.Lock()
expected_lock = threading.Lock()
kafka_lock = threading.Lock()
stop = threading.Event()
background_errors = []
threads = []
cache = archive = consumer = admin = None
message_observer = None
last_sequence = 0


def session():
    client = requests.Session()
    client.trust_env = False
    return client


def request(client, method, path, **kwargs):
    result = client.request(method, base + path, timeout=10, **kwargs)
    result.raise_for_status()
    return result


def summarize(values):
    ordered = sorted(values)
    return {'count': len(ordered), 'mean_ms': round(statistics.mean(ordered), 3),
            'p50_ms': round(statistics.median(ordered), 3),
            'p95_ms': round(ordered[math.ceil(len(ordered) * .95) - 1], 3),
            'p99_ms': round(ordered[math.ceil(len(ordered) * .99) - 1], 3),
            'max_ms': round(ordered[-1], 3)}


def lag():
    with kafka_lock:
        committed = {group: admin.list_consumer_group_offsets(group, partitions=partitions) for group in groups}
        ends = consumer.end_offsets(partitions)
        result = {group: sum(ends[p] - committed[group][p].offset for p in partitions) for group in groups}
    return result


def drain():
    global last_sequence
    begin = time.perf_counter()
    while time.perf_counter() - begin < 60:
        with archive.cursor() as cursor:
            cursor.execute('SELECT sequence_id,event_json FROM change_event WHERE sequence_id>%s '
                           'AND source_id=%s AND database_name=%s AND table_name=%s ORDER BY sequence_id',
                           (last_sequence, cfg('platform.source-id'), cfg('platform.database'), 'departments'))
            rows = cursor.fetchall()
        for sequence, payload in rows:
            last_sequence = sequence
            code = (json.loads(payload).get('afterData') or {}).get('code', '')
            if code.startswith(tag):
                archived_codes.add(code)
        missing = expected - archived_codes
        current_lag = lag()
        if not missing and all(value == 0 for value in current_lag.values()):
            return {'seconds': round(time.perf_counter() - begin, 3), 'lag': current_lag,
                    'expected_versions': len(expected), 'archived_versions': len(expected & archived_codes)}
        time.sleep(.1)
    raise TimeoutError('CDC did not drain within 60s; missing_versions=' + str(len(missing)) + '; lag=' + str(current_lag))


def observe_cache():
    try:
        while not stop.is_set():
            with active_lock:
                pending = list(active.items())
            if pending:
                pipe = cache.pipeline(transaction=False)
                for key, _ in pending:
                    pipe.exists(key)
                exists = pipe.execute()
                observed = time.perf_counter()
                for (_, state), present in zip(pending, exists):
                    if not present and not state['done'].is_set():
                        state['elapsed_ms'] = (observed - state['start']) * 1000
                        state['done'].set()
            stop.wait(.005)
    except Exception as error:
        background_errors.append(repr(error))
        stop.set()


def observe_resources():
    previous = {}
    try:
        while not stop.is_set():
            now = time.perf_counter()
            point = {'elapsed_seconds': round(now - started, 3), 'containers': {}, 'lag': lag()}
            for name, path in cgroups.items():
                cpu = dict(line.split() for line in (path / 'cpu.stat').read_text().splitlines())
                usage = int(cpu['usage_usec'])
                item = {'memory_mib': round(int((path / 'memory.current').read_text()) / 1048576, 2),
                        'swap_mib': round(int((path / 'memory.swap.current').read_text()) / 1048576, 2)}
                if name in previous:
                    at, old = previous[name]
                    item['cpu_percent_one_core'] = round((usage - old) / ((now - at) * 10000), 2)
                previous[name] = (now, usage)
                point['containers'][name] = item
            memory = dict(line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
            point['host_available_mib'] = round(int(memory['MemAvailable'].split()[0]) / 1024, 2)
            point['pressure'] = {name: Path('/proc/pressure/' + name).read_text().strip() for name in ['cpu', 'io', 'memory']}
            vm = dict(line.split() for line in Path('/proc/vmstat').read_text().splitlines())
            point['vm_counters'] = {name: int(vm[name]) for name in ['pgmajfault', 'pswpin', 'pswpout']}
            report['resource_samples'].append(point)
            stop.wait(1)
    except Exception as error:
        background_errors.append(repr(error))
        stop.set()


def observe_messages():
    marker = re.compile(re.escape(tag.encode()) + rb'-w\d{2}-\d{6}')
    try:
        while not stop.is_set():
            batches = message_observer.poll(timeout_ms=20)
            received = time.perf_counter()
            with kafka_condition:
                for records in batches.values():
                    for record in records:
                        for code in set(marker.findall(record.value)):
                            kafka_seen.setdefault(code.decode(), {
                                'observed_at': received, 'record_timestamp_ms': record.timestamp,
                                'timestamp_type': record.timestamp_type,
                                'partition': record.partition, 'offset': record.offset})
                kafka_condition.notify_all()
    except Exception as error:
        background_errors.append(repr(error))
        stop.set()


def jvm_counters(path):
    data = path.read_bytes()
    endian = '<' if data[4] == 1 else '>'
    at, count = struct.unpack_from(endian + 'ii', data, 24)
    counters = {}
    for _ in range(count):
        length, name_at, vector = struct.unpack_from(endian + 'iii', data, at)
        kind = chr(data[at + 12])
        value_at = struct.unpack_from(endian + 'i', data, at + 16)[0]
        name = data[at + name_at:].split(b'\0', 1)[0].decode()
        if name.startswith(('sun.gc.collector.', 'sun.os.hrt.', 'sun.rt.safepoint')):
            if kind == 'J':
                counters[name] = struct.unpack_from(endian + 'q', data, at + value_at)[0]
            elif kind == 'B':
                counters[name] = data[at + value_at:at + value_at + vector].split(b'\0', 1)[0].decode()
        at += length
    return counters


def observe_gc():
    previous = report['jvm_before'].copy()
    try:
        while not stop.is_set():
            for name, path in jvm_paths.items():
                counters = jvm_counters(path)
                wall_ms = time.time() * 1000
                frequency = counters['sun.os.hrt.frequency']
                for key, exit_ticks in counters.items():
                    if not key.endswith('.lastExitTime') or exit_ticks == previous[name][key]:
                        continue
                    prefix = key.removesuffix('lastExitTime')
                    entry = counters[prefix + 'lastEntryTime']
                    if exit_ticks >= entry:
                        report['jvm_gc_events'].append({
                            'container': name, 'collector': counters[prefix + 'name'],
                            'pause_ms': (exit_ticks - entry) / frequency * 1000,
                            'start_epoch_ms': wall_ms - (counters['sun.os.hrt.ticks'] - entry) / frequency * 1000,
                            'end_epoch_ms': wall_ms - (counters['sun.os.hrt.ticks'] - exit_ticks) / frequency * 1000,
                            'invocations': counters[prefix + 'invocations'],
                            'invocation_delta': counters[prefix + 'invocations'] - previous[name][prefix + 'invocations']})
                previous[name] = counters
            stop.wait(.1)
    except Exception as error:
        background_errors.append(repr(error))
        stop.set()


def run_workers(count, iterations=None, duration=None):
    barrier = threading.Barrier(count + 1)
    clock = {}

    def work(index):
        client = session()
        row_id = owned[index]
        path = '/departments/' + str(row_id)
        key = 'department:' + str(row_id)
        samples = []
        try:
            barrier.wait()
            for iteration in range(iterations) if iterations is not None else iter(int, 1):
                if stop.is_set() or (duration is not None and time.perf_counter() >= clock['end']):
                    break
                if iterations is not None:
                    old = request(client, 'GET', path).json()
                    raw = cache.hget(key, 'data')
                    if old['code'] != versions[index]['code'] or raw is None or json.loads(raw)['code'] != old['code']:
                        raise AssertionError('Pre-write cached version mismatch')
                number = versions[index]['number'] + 1
                code = tag + f'-w{index:02d}-{number:06d}'
                state = {'start': time.perf_counter(), 'wall_start': time.time(), 'done': threading.Event()}
                if iterations is not None:
                    with active_lock:
                        active[key] = state
                try:
                    request(client, 'PUT', path, json={'name': code, 'code': code})
                    write_ms = (time.perf_counter() - state['start']) * 1000
                    versions[index] = {'code': code, 'number': number}
                    with expected_lock:
                        expected.add(code)
                    sample = {'write_response_ms': round(write_ms, 3), 'code': code,
                              'started_seconds': round(state['start'] - started, 6),
                              'started_epoch_ms': round(state['wall_start'] * 1000, 3)}
                    if iterations is not None:
                        if not state['done'].wait(20):
                            raise TimeoutError('Cache invalidation not observed within 20s')
                        fresh = request(client, 'GET', path).json()
                        if fresh['code'] != code or fresh['name'] != code:
                            raise AssertionError('Fresh HTTP read mismatch after invalidation')
                        sample.update({'cache_invalidated_ms': round(state['elapsed_ms'], 3),
                                       'extra_wait_after_write_response_ms': round(max(0, state['elapsed_ms'] - write_ms), 3),
                                       'new_version_verified': True})
                    if args.observe_kafka:
                        with kafka_condition:
                            if not kafka_condition.wait_for(lambda: code in kafka_seen, timeout=20):
                                raise TimeoutError('Kafka version not observed within 20s')
                            seen = kafka_seen[code]
                        observed_ms = (seen['observed_at'] - state['start']) * 1000
                        created_ms = seen['record_timestamp_ms'] - state['wall_start'] * 1000
                        sample.update({'kafka_observed_ms': round(observed_ms, 3),
                                       'producer_record_created_ms': round(created_ms, 3),
                                       'write_response_to_kafka_observed_ms': round(observed_ms - write_ms, 3),
                                       'producer_created_to_kafka_observed_ms': round(observed_ms - created_ms, 3),
                                       'kafka_record': seen})
                        if iterations is not None:
                            sample['kafka_observed_to_cache_invalidated_ms'] = round(state['elapsed_ms'] - observed_ms, 3)
                    samples.append(sample)
                finally:
                    if iterations is not None:
                        with active_lock:
                            active.pop(key)
        except Exception:
            stop.set()
            raise
        finally:
            client.close()
        return samples

    begin = time.perf_counter()
    with ThreadPoolExecutor(max_workers=count) as pool:
        futures = [pool.submit(work, index) for index in range(count)]
        clock['end'] = time.perf_counter() + (duration or 0)
        begin = time.perf_counter()
        barrier.wait()
        results = [sample for future in futures for sample in future.result()]
    elapsed = time.perf_counter() - begin
    if background_errors:
        raise RuntimeError('Observation failed: ' + '; '.join(background_errors))
    return {'workers': count, 'elapsed_seconds': round(elapsed, 3), 'successful_updates': len(results),
            'updates_per_second': round(len(results) / elapsed, 3), 'samples': results,
            'statistics': {key: summarize([sample[key] for sample in results]) for key in results[0]
                           if key.endswith('_ms') and key != 'started_epoch_ms'}}


try:
    client = session()
    report['health_before'] = request(client, 'GET', '/actuator/health').json()
    report['consumers_before'] = request(client, 'GET', '/platform/consumers/status').json()
    try:
        request(client, 'DELETE', '/diagnostics/traces')
    except Exception:
        pass
    groups = [report['consumers_before']['archive']['groupId'], cfg('canal.mq.kafka.group-id')]
    consumer = KafkaConsumer(bootstrap_servers=args.kafka_bootstrap, group_id=None, enable_auto_commit=False)
    admin = KafkaAdminClient(bootstrap_servers=args.kafka_bootstrap, client_id='cdc-load-observer')
    partitions = [TopicPartition('example', p) for p in sorted(consumer.partitions_for_topic('example'))]
    report['kafka_begin_offsets'] = {str(p.partition): offset for p, offset in consumer.end_offsets(partitions).items()}
    archive = helper.connection(cfg, 'platform.datasource')
    cache = redis.Redis(host=cfg('spring.data.redis.host'), port=int(cfg('spring.data.redis.port')),
                        db=int(cfg('spring.data.redis.database')), password=cfg('spring.data.redis.password') or None,
                        socket_timeout=4, socket_connect_timeout=4, decode_responses=True)
    with archive.cursor() as cursor:
        cursor.execute('SELECT COALESCE(MAX(sequence_id),0) FROM change_event')
        last_sequence = cursor.fetchone()[0]
    cgroups = {}
    jvm_paths = {}
    for name in ['cdc-platform-app', 'canal-server', 'kafka', 'mysql', 'redis']:
        info = json.loads(subprocess.check_output(['docker', 'inspect', name]))[0]
        cgroup = Path('/proc/' + str(info['State']['Pid']) + '/cgroup').read_text().strip().split('::', 1)[1]
        cgroups[name] = Path('/sys/fs/cgroup') / cgroup.lstrip('/')
        if args.observe_jvm and name in ['cdc-platform-app', 'canal-server', 'kafka']:
            jvm_paths[name] = next((Path('/proc/' + str(info['State']['Pid']) + '/root/tmp')).glob('hsperfdata_*/*'))
    for index in range(max(args.workers)):
        code = tag + f'-w{index:02d}-000000'
        row_id = int(request(client, 'POST', '/departments', json={'name': code, 'code': code}).json()['id'])
        owned.append(row_id)
        versions[index] = {'code': code, 'number': 0}
        expected.add(code)
    report['temporary_department_ids'] = owned
    report['initial_drain'] = drain()
    observers = [observe_cache, observe_resources]
    if args.observe_jvm:
        report['jvm_before'] = {name: jvm_counters(path) for name, path in jvm_paths.items()}
        report['jvm_gc_events'] = []
        observers.append(observe_gc)
    if args.observe_kafka:
        message_observer = KafkaConsumer(bootstrap_servers=args.kafka_bootstrap, group_id=None,
                                         enable_auto_commit=False, client_id='cdc-tail-observer',
                                         fetch_min_bytes=1, fetch_max_wait_ms=50)
        message_observer.assign(partitions)
        for partition, offset in message_observer.end_offsets(partitions).items():
            message_observer.seek(partition, offset)
        report['kafka_observer'] = {'group_id': None, 'commits_offsets': False,
                                    'matching': 'First raw protobuf record containing the unique new version'}
        observers.append(observe_messages)
    for function in observers:
        thread = threading.Thread(target=function)
        thread.start()
        threads.append(thread)
    for workers in args.workers:
        run_workers(workers, iterations=args.warmup)
        phase = {'mode': 'closed_loop', 'started_seconds': round(time.perf_counter() - started, 3)}
        phase.update(run_workers(workers, iterations=args.iterations))
        phase['ended_seconds'] = round(time.perf_counter() - started, 3)
        phase['drain'] = drain()
        report['phases'].append(phase)
        print(json.dumps({key: value for key, value in phase.items() if key != 'samples'}), flush=True)
    if not args.skip_write_burst:
        for index, row_id in enumerate(owned):
            request(client, 'GET', '/departments/' + str(row_id))
        phase = {'mode': 'write_only', 'started_seconds': round(time.perf_counter() - started, 3)}
        phase.update(run_workers(max(args.workers), duration=args.write_seconds))
        phase['ended_seconds'] = round(time.perf_counter() - started, 3)
        phase['drain'] = drain()
        for index, row_id in enumerate(owned):
            fresh = request(client, 'GET', '/departments/' + str(row_id)).json()
            if fresh['code'] != versions[index]['code']:
                raise AssertionError('Final version mismatch after write-only phase')
        phase['all_final_versions_verified'] = True
        report['phases'].append(phase)
        print(json.dumps({key: value for key, value in phase.items() if key != 'samples'}), flush=True)
    report['consumers_after'] = request(client, 'GET', '/platform/consumers/status').json()
    report['kafka_end_offsets'] = {str(p.partition): offset for p, offset in consumer.end_offsets(partitions).items()}
    try:
        traces_summary = request(client, 'GET', '/diagnostics/traces/summary?prefix=' + tag).json()
        report['diagnostics_traces_summary'] = traces_summary
    except Exception as ex:
        report['diagnostics_traces_summary'] = {'error': str(ex)}
    report['completed'] = True
except Exception as error:
    report['failure'] = {'type': type(error).__name__, 'message': str(error)[:500]}
finally:
    stop.set()
    for thread in threads:
        thread.join(timeout=15)
    if args.observe_jvm and 'jvm_before' in report:
        report['jvm_after'] = {name: jvm_counters(path) for name, path in jvm_paths.items()}
    try:
        business = helper.connection(cfg, 'spring.datasource')
        with business.cursor() as cursor:
            cursor.execute('DELETE FROM departments WHERE name LIKE %s AND code LIKE %s', (tag + '%', tag + '%'))
            deleted = cursor.rowcount
            cursor.execute('SELECT COUNT(*) FROM departments WHERE name LIKE %s AND code LIKE %s', (tag + '%', tag + '%'))
            remaining = cursor.fetchone()[0]
        business.close()
        deadline = time.perf_counter() + 30
        while owned and cache.exists(*['department:' + str(row_id) for row_id in owned]) and time.perf_counter() < deadline:
            time.sleep(.05)
        report['cleanup'] = {'deleted_rows': deleted, 'remaining_business_rows': remaining,
                             'remaining_cache_keys': cache.exists(*['department:' + str(row_id) for row_id in owned]) if owned else 0}
    except Exception as error:
        report['cleanup'] = {'failure': repr(error)}
    for connection in [archive, cache, consumer, admin, message_observer]:
        if connection is not None:
            connection.close()
    report['finished_at'] = datetime.now(timezone.utc).isoformat()
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'label': args.label, 'completed': report['completed'], 'failure': report.get('failure'),
                      'cleanup': report.get('cleanup'), 'report': args.output}), flush=True)
if not report['completed'] or report['cleanup'].get('remaining_business_rows') != 0 or report['cleanup'].get('remaining_cache_keys') != 0:
    raise SystemExit(1)
