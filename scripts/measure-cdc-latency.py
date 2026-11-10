"""Measure sequential write-to-cache-invalidation and archive visibility latency.

Requires requests, PyYAML, PyMySQL and redis; optional Kafka probes need kafka-python.
Creates one uniquely tagged
department and deletes it on exit; archived events remain as test evidence.
"""

import argparse
import concurrent.futures
import importlib.util
import json
import math
from pathlib import Path
import statistics
import threading
import time
import uuid
from datetime import datetime, timezone, timedelta

import redis
import requests


parser = argparse.ArgumentParser()
parser.add_argument('--project', required=True)
parser.add_argument('--output', required=True)
parser.add_argument('--base-url', default='http://127.0.0.1:8081')
parser.add_argument('--samples', type=int, default=50)
parser.add_argument('--warmup', type=int, default=5)
parser.add_argument('--kafka-bootstrap', help='Observe Kafka independently, without joining or committing a consumer group')
parser.add_argument('--kafka-topic', default='example')
args = parser.parse_args()
project = Path(args.project)
spec = importlib.util.spec_from_file_location('acceptance_config', project / 'scripts/stockout-acceptance.py')
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)
cfg = helper.configuration()
session = requests.Session()
session.trust_env = False
base = args.base_url.rstrip('/')
tag = 'cdc-lat-' + uuid.uuid4().hex[:10]
report = {
    'started_at': datetime.now(timezone(timedelta(hours=8))).isoformat(),
    'run_id': tag,
    'base_url': base,
    'method': 'Sequential HTTP PUTs; one local monotonic clock; parallel direct Redis and platform MySQL probes.',
    'timing_origin': 'Immediately before submitting the HTTP PUT; includes HTTP, database, network and probe overhead.',
    'probe_pause_seconds': 0.005,
    'planned_samples': args.samples,
    'planned_warmup': args.warmup,
    'samples': [],
    'warmup_samples': [],
    'completed': False,
}
row_id = None
creation_started = False
archive = None
cache = None
kafka = None
last_sequence = 0


def request(method, path, **kwargs):
    response = session.request(method, base + path, timeout=15, **kwargs)
    response.raise_for_status()
    return response


def archived(operation, code, after_sequence, clock, gate):
    gate.wait()
    deadline = time.perf_counter() + 20
    while time.perf_counter() < deadline:
        with archive.cursor() as cursor:
            cursor.execute(
                'SELECT sequence_id,event_id,event_json FROM change_event '
                'WHERE source_id=%s AND database_name=%s AND table_name=%s '
                'AND primary_key_value=%s AND operation=%s AND sequence_id>%s '
                'ORDER BY sequence_id DESC LIMIT 4',
                (cfg('platform.source-id'), cfg('platform.database'), 'departments', str(row_id), operation, after_sequence),
            )
            rows = cursor.fetchall()
        for sequence, event_id, payload in rows:
            event = json.loads(payload)
            data = event.get('afterData') or event.get('beforeData') or {}
            if data.get('code') == code:
                return {'sequence_id': sequence, 'event_id': event_id,
                        'elapsed_ms': (time.perf_counter() - clock['start']) * 1000}
        time.sleep(0.005)
    raise TimeoutError('No matching archived ' + operation + ' event within 20 seconds')


def invalidated(clock, gate):
    gate.wait()
    deadline = time.perf_counter() + 20
    while time.perf_counter() < deadline:
        if not cache.exists('department:' + str(row_id)):
            return (time.perf_counter() - clock['start']) * 1000
        time.sleep(0.005)
    raise TimeoutError('Redis cache was not invalidated within 20 seconds')


def kafka_observed(code, clock, gate):
    gate.wait()
    marker = code.encode('utf-8')
    deadline = time.perf_counter() + 20
    while time.perf_counter() < deadline:
        batches = kafka.poll(timeout_ms=10)
        observed = time.perf_counter()
        for records in batches.values():
            for record in records:
                if marker in record.value:
                    return {
                        'elapsed_ms': (observed - clock['start']) * 1000,
                        'partition': record.partition, 'offset': record.offset,
                        'timestamp_type': record.timestamp_type,
                        'record_timestamp_ms': record.timestamp,
                        'producer_created_ms': record.timestamp - clock['wall_start'] * 1000,
                    }
    raise TimeoutError('No Kafka record containing the unique version within 20 seconds')


def summary(values):
    ordered = sorted(values)
    return {
        'count': len(values),
        'mean_ms': round(statistics.mean(values), 3),
        'p50_ms': round(statistics.median(values), 3),
        'p95_ms': round(ordered[math.ceil(0.95 * len(ordered)) - 1], 3),
        'min_ms': round(ordered[0], 3),
        'max_ms': round(ordered[-1], 3),
    }


try:
    health = request('GET', '/actuator/health').json()
    report['health_before'] = health
    report['consumer_status_before'] = request('GET', '/platform/consumers/status').json()
    archive = helper.connection(cfg, 'platform.datasource')
    cache = redis.Redis(host=cfg('spring.data.redis.host'), port=int(cfg('spring.data.redis.port')),
                        db=int(cfg('spring.data.redis.database')), password=cfg('spring.data.redis.password') or None,
                        socket_timeout=4, socket_connect_timeout=4, decode_responses=True)
    cache.ping()
    if args.kafka_bootstrap:
        from kafka import KafkaConsumer, TopicPartition
        kafka = KafkaConsumer(bootstrap_servers=args.kafka_bootstrap, group_id=None,
                              enable_auto_commit=False, client_id='cdc-latency-observer',
                              fetch_min_bytes=1, fetch_max_wait_ms=50)
        partitions = sorted(kafka.partitions_for_topic(args.kafka_topic))
        assigned = [TopicPartition(args.kafka_topic, partition) for partition in partitions]
        kafka.assign(assigned)
        for partition, offset in kafka.end_offsets(assigned).items():
            kafka.seek(partition, offset)
        report['kafka_observer'] = {'bootstrap': args.kafka_bootstrap, 'topic': args.kafka_topic,
                                    'partitions': partitions, 'group_id': None, 'commits_offsets': False,
                                    'matching': 'First raw Canal protobuf record containing the unique new version'}
    creation_started = True
    created = request('POST', '/departments', json={'name': tag + '-000', 'code': tag + '-000'})
    row_id = int(created.json()['id'])
    report['temporary_department_id'] = row_id
    gate = threading.Event()
    clock = {'start': time.perf_counter()}
    gate.set()
    initial = archived('INSERT', tag + '-000', 0, clock, gate)
    last_sequence = initial['sequence_id']
    time.sleep(1)
    request('GET', '/departments/' + str(row_id))
    with concurrent.futures.ThreadPoolExecutor(max_workers=3 if kafka else 2) as pool:
        for index in range(1, args.warmup + args.samples + 1):
            old_code = tag + f'-{index - 1:03d}'
            new_code = tag + f'-{index:03d}'
            old = request('GET', '/departments/' + str(row_id)).json()
            if old.get('code') != old_code:
                raise AssertionError('Pre-write HTTP read did not return the preceding version')
            raw = cache.hget('department:' + str(row_id), 'data')
            if raw is None or json.loads(raw).get('code') != old_code:
                raise AssertionError('The previous version was not present in Redis before PUT')
            gate = threading.Event()
            clock = {'start': 0.0}
            cache_future = pool.submit(invalidated, clock, gate)
            archive_future = pool.submit(archived, 'UPDATE', new_code, last_sequence, clock, gate)
            if kafka:
                kafka_future = pool.submit(kafka_observed, new_code, clock, gate)
            clock['start'] = time.perf_counter()
            clock['wall_start'] = time.time()
            gate.set()
            request('PUT', '/departments/' + str(row_id), json={'name': new_code, 'code': new_code})
            write_ms = (time.perf_counter() - clock['start']) * 1000
            cache_ms = cache_future.result()
            fresh = request('GET', '/departments/' + str(row_id)).json()
            fresh_ms = (time.perf_counter() - clock['start']) * 1000
            if fresh.get('code') != new_code or fresh.get('name') != new_code:
                raise AssertionError('Read after cache invalidation did not return the written version')
            event = archive_future.result()
            last_sequence = event['sequence_id']
            sample = {
                'iteration': index,
                'write_response_ms': round(write_ms, 3),
                'cache_invalidated_ms': round(cache_ms, 3),
                'archive_visible_ms': round(event['elapsed_ms'], 3),
                'fresh_read_complete_ms': round(fresh_ms, 3),
                'extra_wait_after_write_response_ms': round(max(0.0, cache_ms - write_ms), 3),
                'new_version_verified': True,
                'event_id': event['event_id'],
            }
            if kafka:
                observed = kafka_future.result()
                sample.update({
                    'kafka_observed_ms': round(observed['elapsed_ms'], 3),
                    'write_response_to_kafka_observed_ms': round(observed['elapsed_ms'] - write_ms, 3),
                    'kafka_observed_to_cache_invalidated_ms': round(cache_ms - observed['elapsed_ms'], 3),
                    'producer_record_created_ms': round(observed['producer_created_ms'], 3),
                    'write_response_to_producer_record_created_ms': round(observed['producer_created_ms'] - write_ms, 3),
                    'producer_record_created_to_kafka_observed_ms': round(observed['elapsed_ms'] - observed['producer_created_ms'], 3),
                    'kafka_record': observed,
                })
            bucket = 'warmup_samples' if index <= args.warmup else 'samples'
            report[bucket].append(sample)
            formal = len(report['samples'])
            if index == args.warmup or (formal and formal % 10 == 0):
                print(json.dumps({'warmup_done': len(report['warmup_samples']), 'samples_done': formal,
                                  'latest_cache_ms': sample['cache_invalidated_ms'],
                                  'latest_archive_ms': sample['archive_visible_ms']}), flush=True)
    metrics = ['write_response_ms', 'cache_invalidated_ms', 'archive_visible_ms',
               'fresh_read_complete_ms', 'extra_wait_after_write_response_ms']
    if kafka:
        metrics += ['kafka_observed_ms', 'write_response_to_kafka_observed_ms',
                    'kafka_observed_to_cache_invalidated_ms', 'producer_record_created_ms',
                    'write_response_to_producer_record_created_ms',
                    'producer_record_created_to_kafka_observed_ms']
    report['statistics'] = {key: summary([sample[key] for sample in report['samples']]) for key in metrics}
    report['consumer_status_after'] = request('GET', '/platform/consumers/status').json()
    report['completed'] = True
except Exception as error:
    report['failure'] = {'type': type(error).__name__, 'message': str(error)[:500]}
finally:
    if creation_started:
        try:
            business = helper.connection(cfg, 'spring.datasource')
            with business.cursor() as cursor:
                cursor.execute('SELECT id FROM departments WHERE name LIKE %s AND code LIKE %s', (tag + '%', tag + '%'))
                owned_ids = [row[0] for row in cursor.fetchall()]
                cursor.execute('DELETE FROM departments WHERE name LIKE %s AND code LIKE %s', (tag + '%', tag + '%'))
                deleted = cursor.rowcount
                cursor.execute('SELECT COUNT(*) FROM departments WHERE name LIKE %s AND code LIKE %s', (tag + '%', tag + '%'))
                remaining = cursor.fetchone()[0]
            business.close()
            cleanup = {'deleted_rows': deleted, 'remaining_business_rows': remaining}
            if row_id is None and len(owned_ids) == 1:
                row_id = owned_ids[0]
            if row_id is not None and archive is not None and cache is not None:
                gate = threading.Event()
                clock = {'start': time.perf_counter()}
                gate.set()
                invalidated(clock, gate)
                cleanup['redis_key_remaining'] = bool(cache.exists('department:' + str(row_id)))
            report['cleanup'] = cleanup
        except Exception as error:
            report['cleanup'] = {'ok': False, 'type': type(error).__name__, 'message': str(error)[:500]}
    if archive is not None:
        archive.close()
    if cache is not None:
        cache.close()
    if kafka is not None:
        kafka.close()
    report['finished_at'] = datetime.now(timezone(timedelta(hours=8))).isoformat()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({'completed': report['completed'], 'statistics': report.get('statistics'),
                      'failure': report.get('failure'), 'cleanup': report.get('cleanup'),
                      'report': str(output)}, ensure_ascii=True), flush=True)
if not report['completed'] or report.get('cleanup', {}).get('remaining_business_rows') != 0:
    raise SystemExit(1)
