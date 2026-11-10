"""Real CDC acceptance: existing middleware, an owned local JVM and a local fault gateway.

Requires Python requests, PyYAML and PyMySQL, plus `mvn test` to produce classes/classpath.
Creates uniquely tagged inventory rows and one subscription; disables the subscription and
removes only those inventory rows on exit. Archive, delivery and stockout evidence is retained.
No middleware is stopped. Credentials are read from local config and never exported.
"""
import argparse
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit
import uuid
import xml.etree.ElementTree as ET

import pymysql
import requests
import yaml

ROOT = Path(__file__).resolve().parents[1]


def configuration():
    values = {}
    def flatten(prefix, node):
        for key, value in node.items():
            name = prefix + key
            if isinstance(value, dict):
                flatten(name + '.', value)
            else:
                values[name] = value
    for relative in ['src/main/resources/application.yml', 'src/main/resources/application-local.yml', 'application-local.yml']:
        path = ROOT / relative
        if path.exists():
            flatten('', yaml.safe_load(path.read_text(encoding='utf-8-sig')))
    def resolve(name):
        value = str(values[name])
        while '${' in value:
            def replace(match):
                key, separator, default = match[1].partition(':')
                if key in os.environ:
                    return os.environ[key]
                if key in values:
                    return resolve(key)
                if separator:
                    return default
                raise ValueError('Unresolved configuration property: ' + key)
            updated = re.sub(r'\$\{([^{}]+)\}', replace, value)
            if updated == value:
                raise ValueError('Cannot resolve configuration property: ' + name)
            value = updated
        return value
    return resolve


def connection(resolve, prefix):
    url = urlsplit(resolve(prefix + '.url').removeprefix('jdbc:'))
    return pymysql.connect(host=url.hostname, port=url.port or 3306, database=url.path.lstrip('/'),
                           user=resolve(prefix + '.username'), password=resolve(prefix + '.password'),
                           charset='utf8mb4', autocommit=True, connect_timeout=5, read_timeout=15)


def runtime_classpath():
    report = next((ROOT / 'target/surefire-reports').glob('TEST-*.xml'))
    properties = ET.parse(report).getroot().find('properties')
    classpath = next(p.attrib['value'] for p in properties if p.attrib['name'] == 'java.class.path')
    return os.pathsep.join(p for p in classpath.split(os.pathsep)
                           if not p.replace('\\', '/').endswith('/target/test-classes'))


def wait_for(description, check, timeout=60):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = check()
        if last:
            return last
        time.sleep(0.25)
    raise AssertionError('Timed out: ' + description + '; last=' + repr(last))


class FaultGateway(BaseHTTPRequestHandler):
    mode = 'NORMAL'
    destination = ''
    sku = ''
    receipts = []
    forwarded = threading.Event()
    release = threading.Event()

    def log_message(self, *_):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers['Content-Length']))
        payload = json.loads(body)
        if payload['event']['afterData']['sku'] != self.sku:
            self.send_error(400, 'Not this acceptance run')
            return
        mode = type(self).mode
        if mode == 'UNAVAILABLE':
            self.send_error(503, 'Acceptance receiver unavailable')
            return
        result = requests.post(self.destination, data=body, headers={'Content-Type': 'application/json'}, timeout=8)
        type(self).receipts.append({'eventId': payload['eventId'], 'deliveryId': payload['deliveryId'],
                                   'status': result.status_code, 'mode': mode})
        if mode == 'DROP_ONCE' and result.ok:
            type(self).mode = 'NORMAL'
            # Business commit has completed, but the HTTP acknowledgement is lost.
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        if mode == 'HOLD_AFTER_COMMIT' and result.ok:
            type(self).forwarded.set()
            type(self).release.wait(20)
        try:
            self.send_response(result.status_code)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(result.content)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
            pass  # Expected when the owned JVM is terminated after business commit.


class Acceptance:
    def __init__(self, args):
        self.args = args
        self.run_id = 'stockout-' + time.strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:6]
        self.base = f'http://127.0.0.1:{args.app_port}'
        self.resolve = configuration()
        self.business = connection(self.resolve, 'spring.datasource')
        self.platform = connection(self.resolve, 'platform.datasource')
        self.app = None
        self.log = None
        self.starts = 0
        self.subscription = None
        self.rows = []
        self.evidence = {'runId': self.run_id, 'checks': [], 'inventoryIds': self.rows}
        self.output = args.output.resolve()
        self.output.parent.mkdir(parents=True, exist_ok=True)
        FaultGateway.destination = self.base + '/warehouse/stockout-events'
        FaultGateway.sku = self.run_id
        self.gateway = ThreadingHTTPServer(('127.0.0.1', args.gateway_port), FaultGateway)
        self.gateway.daemon_threads = True
        threading.Thread(target=self.gateway.serve_forever, daemon=True).start()

    def api(self, method, path, body=None):
        response = requests.request(method, self.base + path, json=body, timeout=10)
        response.raise_for_status()
        return response.json()

    def healthy(self):
        if self.app.poll() is not None:
            raise RuntimeError('Owned application exited; inspect ' + self.log.name)
        try:
            return requests.get(self.base + '/actuator/health', timeout=2).status_code == 200
        except requests.ConnectionError:
            return False

    def start(self):
        self.starts += 1
        self.log = open(self.output.parent / f'{self.run_id}-app-{self.starts}.log', 'w', encoding='utf-8')
        args = ['java', '-cp', runtime_classpath(), 'com.example.canalcache.CanalCacheSyncApplication',
                '--spring.profiles.active=local,stockout-demo',
                '--server.address=127.0.0.1', f'--server.port={self.args.app_port}', '--platform.enabled=true',
                '--platform.lease-seconds=10', '--platform.backoff-initial-seconds=1',
                f'--platform.targets.stockout-workbench.url={self.base}/warehouse/stockout-events',
                f'--platform.targets.stockout-drill.url=http://127.0.0.1:{self.args.gateway_port}/hook',
                '--platform.targets.stockout-drill.timeout-ms=5000']
        self.app = subprocess.Popen(args, cwd=ROOT, stdout=self.log, stderr=subprocess.STDOUT,
                                    creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        wait_for('application health', self.healthy, 90)
        print('Application ready, owned PID', self.app.pid, flush=True)

    def stop(self):
        if self.app is not None and self.app.poll() is None:
            self.app.terminate()
            self.app.wait(timeout=20)
        if self.log is not None:
            self.log.close()

    def record(self, name, **evidence):
        self.evidence['checks'].append({'name': name, 'passed': True, **evidence})
        self.save()
        print('PASS:', name, flush=True)

    def save(self):
        self.output.write_text(json.dumps(self.evidence, ensure_ascii=False, indent=2), encoding='utf-8')

    def rule(self, sku):
        return {'table': 'inventory', 'targetName': 'stockout-drill', 'operations': ['UPDATE'],
                'changedFields': ['available_qty'], 'filterField': 'sku', 'filterValue': sku, 'enabled': True}

    def deliveries(self, event_id, round_id=None):
        path = '/platform/deliveries?eventId=' + event_id + '&subscriptionId=' + self.subscription['subscriptionId']
        if round_id:
            path += '&roundId=' + round_id
        return self.api('GET', path)['items']

    def status(self, event_id, expected, round_id=None):
        rows = self.deliveries(event_id, round_id)
        return next((t for t in rows if t['status'] == expected), None)

    def stockout(self, event_id):
        items = self.api('GET', '/warehouse/stockout-tasks?eventId=' + event_id)['items']
        assert len(items) <= 1, 'Duplicate business tasks'
        return items[0] if items else None

    def create_stockout(self, label):
        with self.business.cursor() as cur:
            cur.execute('INSERT INTO inventory(sku,available_qty,remark) VALUES (%s,5,%s)', (self.run_id, label))
            row_id = cur.lastrowid
            self.rows.append(row_id)
            cur.execute('UPDATE inventory SET available_qty=0 WHERE id=%s AND sku=%s', (row_id, self.run_id))
        def archived():
            page = self.api('GET', f'/platform/events?table=inventory&key={row_id}&operation=UPDATE&limit=20')
            return page['items'][0] if page['items'] else None
        event = wait_for('real Canal/Kafka archive for ' + label, archived)
        return event['eventId']

    def attempts(self, task):
        return self.api('GET', f"/platform/deliveries/{task['taskId']}/attempts")

    def run(self):
        with self.business.cursor() as cur:
            if self.args.init_stockout:
                sql = (ROOT / 'examples/stockout/schema.sql').read_text(encoding='utf-8')
                cur.execute(sql[sql.index('CREATE TABLE'):].strip().removesuffix(';'))
            cur.execute('SELECT COUNT(*) FROM stockout_task')
            cur.execute('SELECT VERSION(),@@binlog_format,@@binlog_row_image')
            self.evidence['mysql'] = list(cur.fetchone())
        self.start()
        self.subscription = self.api('POST', '/platform/subscriptions', self.rule(self.run_id))
        self.evidence['subscriptionId'] = self.subscription['subscriptionId']

        normal = self.create_stockout('normal')
        task = wait_for('normal delivery', lambda: self.status(normal, 'SUCCEEDED'))
        todo = self.stockout(normal)
        assert todo and todo['status'] == 'PENDING'
        handled = self.api('POST', f"/warehouse/stockout-tasks/{todo['id']}/complete")
        self.record('real CDC creates a persistent task and it can be handled', eventId=normal,
                    deliveryId=task['deliveryId'], stockout=handled)

        request = {'requestKey': self.run_id + '-replay', 'subscriptionId': self.subscription['subscriptionId'], 'eventIds': [normal]}
        job = self.api('POST', '/platform/replay-jobs', request)
        replay = wait_for('replay delivery', lambda: self.status(normal, 'SUCCEEDED', job['jobId']))
        assert replay['deliveryId'] != task['deliveryId']
        assert self.stockout(normal) == handled
        assert self.api('POST', '/platform/replay-jobs', request)['jobId'] == job['jobId']
        assert len(self.deliveries(normal, job['jobId'])) == 1
        self.record('replay uses a new delivery but preserves the one handled business task', job=job,
                    replayDeliveryId=replay['deliveryId'])

        FaultGateway.mode = 'UNAVAILABLE'
        failed_event = self.create_stockout('503-to-manual-retry')
        failed = wait_for('automatic attempts exhausted', lambda: self.status(failed_event, 'FAILED'))
        assert failed['attemptCount'] == 3 and self.stockout(failed_event) is None
        FaultGateway.mode = 'NORMAL'
        self.api('POST', f"/platform/deliveries/{failed['taskId']}/retry")
        fixed = wait_for('manual delivery', lambda: self.status(failed_event, 'SUCCEEDED'))
        assert fixed['deliveryId'] == failed['deliveryId'] and fixed['attemptCount'] == 4
        self.record('three real HTTP 503 failures then one manual successful attempt', eventId=failed_event,
                    attempts=self.attempts(fixed), stockout=self.stockout(failed_event))

        FaultGateway.mode = 'DROP_ONCE'
        lost = self.create_stockout('response-lost-after-business-commit')
        delivered = wait_for('lost acknowledgement retried', lambda: self.status(lost, 'SUCCEEDED'))
        receipts = [r for r in FaultGateway.receipts if r['eventId'] == lost]
        assert len(receipts) >= 2 and delivered['attemptCount'] >= 2 and self.stockout(lost)
        self.record('business commit followed by response loss does not duplicate the task', eventId=lost,
                    receipts=receipts, attempts=self.attempts(delivered), stockout=self.stockout(lost))

        FaultGateway.mode = 'UNAVAILABLE'
        restart_event = self.create_stockout('restart-in-retry-wait')
        waiting = wait_for('retry wait before restart', lambda: self.status(restart_event, 'RETRY_WAIT'))
        self.stop()
        FaultGateway.mode = 'NORMAL'
        self.start()
        resumed = wait_for('retry wait resumes after restart', lambda: self.status(restart_event, 'SUCCEEDED'))
        assert resumed['deliveryId'] == waiting['deliveryId'] and self.stockout(normal) == handled
        self.record('application restart resumes persisted retry and preserves handled tasks', eventId=restart_event,
                    attempts=self.attempts(resumed))

        FaultGateway.forwarded.clear()
        FaultGateway.release.clear()
        FaultGateway.mode = 'HOLD_AFTER_COMMIT'
        inflight_event = self.create_stockout('terminate-after-receiver-commit')
        wait_for('receiver committed before acknowledgement', FaultGateway.forwarded.is_set)
        with self.platform.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute('SELECT task_id,status,attempt_count,lease_owner,lease_until FROM delivery_task WHERE event_id=%s AND subscription_id=%s',
                        (inflight_event, self.subscription['subscriptionId']))
            before_restart = cur.fetchone()
        assert before_restart['status'] == 'IN_FLIGHT' and before_restart['attempt_count'] == 0
        self.stop()
        FaultGateway.mode = 'NORMAL'
        FaultGateway.release.set()
        self.start()
        reclaimed = wait_for('expired lease reclaimed after termination', lambda: self.status(inflight_event, 'SUCCEEDED'))
        receipts = [r for r in FaultGateway.receipts if r['eventId'] == inflight_event]
        assert len(receipts) >= 2 and self.stockout(inflight_event)
        before_restart['lease_until'] = str(before_restart['lease_until'])
        self.record('terminated in-flight delivery is reclaimed without duplicate business work', eventId=inflight_event,
                    beforeRestart=before_restart, receipts=receipts, attempts=self.attempts(reclaimed))

        FaultGateway.mode = 'UNAVAILABLE'
        frozen_job = self.api('POST', '/platform/replay-jobs', {**request, 'requestKey': self.run_id + '-frozen'})
        queued = wait_for('frozen replay queued', lambda: self.status(normal, 'RETRY_WAIT', frozen_job['jobId']))
        sub_id = self.subscription['subscriptionId']
        self.subscription = self.api('PUT', f"/platform/subscriptions/{sub_id}?expectedVersion={self.subscription['ruleVersion']}",
                                     self.rule(self.run_id + '-no-longer-matches'))
        FaultGateway.mode = 'NORMAL'
        frozen = wait_for('frozen replay delivers after subscription edit', lambda: self.status(normal, 'SUCCEEDED', frozen_job['jobId']))
        snapshot = json.loads(frozen['subscriptionSnapshot'])
        assert snapshot['filterValue'] == self.run_id and snapshot['ruleVersion'] < self.subscription['ruleVersion']
        assert self.stockout(normal) == handled
        self.record('queued replay retains its rule and target snapshot after subscription edits', jobId=frozen_job['jobId'],
                    frozenRuleVersion=snapshot['ruleVersion'], currentRuleVersion=self.subscription['ruleVersion'],
                    targetUrl=frozen['targetUrl'], attempts=self.attempts(frozen))
        self.evidence['consumerStatus'] = self.api('GET', '/platform/consumers/status')

    def cleanup(self):
        FaultGateway.mode = 'NORMAL'
        FaultGateway.release.set()
        # Only the subscription and stock rows owned by this run are changed.
        if self.subscription:
            with self.platform.cursor() as cur:
                cur.execute('UPDATE event_subscription SET enabled=0,rule_version=rule_version+1 WHERE subscription_id=%s',
                            (self.subscription['subscriptionId'],))
        with self.business.cursor() as cur:
            for row_id in self.rows:
                cur.execute('DELETE FROM inventory WHERE id=%s AND sku=%s', (row_id, self.run_id))
        self.evidence['cleanup'] = 'Owned subscription disabled and owned inventory rows deleted; evidence retained.'
        self.stop()
        self.gateway.shutdown()
        self.gateway.server_close()
        self.business.close()
        self.platform.close()
        self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--app-port', type=int, default=18081)
    parser.add_argument('--gateway-port', type=int, default=18082)
    parser.add_argument('--init-stockout', action='store_true', help='Create the new business table if absent')
    args = parser.parse_args()
    # Avoid touching an application already listening on the selected test port.
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', args.app_port))
    run = Acceptance(args)
    try:
        run.run()
        run.evidence['passed'] = True
    except Exception as error:
        run.evidence['passed'] = False
        run.evidence['error'] = type(error).__name__ + ': ' + str(error)
        raise
    finally:
        run.cleanup()


if __name__ == '__main__':
    main()
