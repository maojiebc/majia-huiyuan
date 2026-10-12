#!/usr/bin/env python3
"""Generate the current 55-table field contract from auditable sources."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from tools.build_mini_world import field_types, headers
from tools.run_pipeline import parse_pipeline

COLUMNS = ['表名', '表英文名', '分层', '字段序号', '字段名', '字段英文名', '类型', '可空', '粒度键序号', '字段说明', '类型来源', '下游消费', '粒度来源']
NAME_PATTERN = re.compile(r'^[a-z][a-z0-9_]{0,62}$')


def historical_fields(table):
    text = (ROOT / '数据集/结构定义' / (table+'.md')).read_text()
    return dict(re.findall(r'^- (.+?) \(fdId:[^\n]*\n  - 字段类型: (\w+)', text, re.M))


def csv_text(columns, rows):
    stream = io.StringIO(newline='')
    writer = csv.DictWriter(stream, fieldnames=columns, lineterminator='\n')
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


def description(table, name, source):
    special = {
        '手机号': '打码展示文本，如156****0925；不可转数值或补全号码',
        '增量GMV': '需合格对照组；本参考实现无对照组时为NULL',
        '增量ROI': '需合格对照组；不可把关联GMV成本比当增量ROI',
        '数据快照日期': '同批次as_of_date；不使用执行机器时钟',
        '门店版本ID': '门店SCD2版本标识；业务日期匹配生效区间',
        '生效起始日期': '门店版本生效起点（含）',
        '生效截止日期': '门店版本失效边界（不含）',
    }
    if name in special:
        return special[name]
    if '关联' in name and any(x in name for x in ('GMV', '销售', '下单', '订单')):
        return name+'；按文档归因规则关联，不表示因果增量'
    return name+'；定义见'+source


def build_contract(evidence=None):
    spec = json.loads((ROOT / '清单/字段契约来源.json').read_text())
    if evidence is None:
        evidence = json.loads((ROOT / '清单/Spark类型证据.json').read_text())
    runtime = {entry['output']: entry for entry in evidence['etls']}
    flows = parse_pipeline()
    for flow in flows:
        recorded = runtime[flow.output]
        if recorded['path'] != str(flow.path.relative_to(ROOT)):
            raise ValueError('Runtime evidence path differs: '+flow.output)
        hashes = {n['id']: n['sql_sha256'] for n in recorded['nodes']}
        # run_pipeline records the documented SQL before as_of substitution;
        # the report's as_of fixes the executor's permitted date replacement.
        current = {n.id: hashlib.sha256(n.sql.encode()).hexdigest() for n in flow.nodes}
        if hashes != current:
            raise ValueError('Runtime SQL fingerprint differs; rerun Spark before adopting schema: '+flow.output)
    for entry in evidence.get('public_sql', []):
        current_hash = hashlib.sha256((ROOT / entry['path']).read_bytes()).hexdigest()
        if current_hash != entry['sql_sha256']:
            raise ValueError('Public SQL fingerprint differs; rerun Spark: '+entry['path'])
    consumers = {}
    for flow in flows:
        for table in flow.inputs:
            consumers.setdefault(table, []).append(flow.output)
    expected = {path.stem for path in (ROOT / '数据集/数据样本').glob('*.csv')}
    if set(spec['tables']) != expected or len(expected) != 55 or len(runtime) != 25:
        raise ValueError('Table/source inventory differs from the 55-table / 25-output contract')
    field_names = spec['fields']
    if len(field_names) != 497 or len(set(field_names.values())) != 497:
        raise ValueError('English field map must contain 497 distinct translations')
    for value in list(field_names.values())+[t['english'] for t in spec['tables'].values()]:
        if not NAME_PATTERN.fullmatch(value):
            raise ValueError('Invalid English identifier: '+value)
    rows, conflicts = [], []
    for table in sorted(expected):
        table_spec = spec['tables'][table]
        columns = headers(table)
        keys = table_spec['grain']
        if not keys or len(keys) != len(set(keys)) or not set(keys) <= set(columns):
            raise ValueError('Invalid natural grain: '+table)
        historical = historical_fields(table)
        source_path = '数据集/结构定义/'+table+'.md'
        if table in runtime:
            entry = runtime[table]
            fields = entry['schema']['fields']
            if [f['name'] for f in fields] != columns:
                raise ValueError('Runtime schema differs from sample column order: '+table)
            types = {f['name']: f['type'].upper() for f in fields}
            nullable = {f['name']: f['nullable'] for f in fields}
            provenance = '清单/Spark类型证据.json#'+table+'；Spark '+evidence['spark_version']+'实际schema；SQL '+entry['path']
            source_path = entry['path']
        else:
            types, nullable = field_types(table), dict.fromkeys(columns, True)
            provenance = ''
        downstream = '；'.join(sorted(consumers.get(table, [])))
        if not downstream:
            downstream = '无（参考结构）' if table not in runtime else '无ETL下游（应用输出）'
        for index, name in enumerate(columns, 1):
            if table in runtime:
                type_source = provenance
            elif table == 'dim_会员主档' and name == '手机号':
                type_source = source_path+'历史LONG与打码文本不兼容；已裁定STRING；tools/build_mini_world.py:field_types显式例外'
            elif name in historical:
                type_source = source_path+'#字段列表（历史导出）'
            else:
                type_source = 'tools/build_mini_world.py:field_types（当前新增列类型；历史导出未包含）'
            if name in historical and historical[name] != types[name]:
                conflicts.append({'表名': table, '字段名': name, '历史类型': historical[name], '当前类型': types[name], '采用依据': type_source})
            rows.append(dict(zip(COLUMNS, [table, table_spec['english'], table.split('_')[0].upper(), index, name, field_names[name], types[name], '是' if nullable[name] else '否', keys.index(name)+1 if name in keys else '', description(table, name, source_path), type_source, downstream, source_path+'；'+table_spec['grain_source']])))
    if len(rows) != 866:
        raise ValueError('Expected 866 columns, got '+str(len(rows)))
    return {'清单/字段契约.csv': csv_text(COLUMNS, rows), 'DDL/类型冲突.csv': csv_text(['表名', '字段名', '历史类型', '当前类型', '采用依据'], conflicts)}


def write_or_check(outputs, check=False, root=ROOT):
    drift = []
    for relative, content in outputs.items():
        path = Path(root) / relative
        if check:
            if not path.exists() or path.read_text() != content:
                drift.append(relative)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
    if drift:
        print('Generated output drift: '+', '.join(drift), file=sys.stderr)
        return 1
    print('Verified' if check else 'Generated', ', '.join(outputs))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    parser.add_argument('--spark-report', type=Path, help='Refresh the auditable schema evidence from a successful fresh pipeline run')
    args = parser.parse_args()
    if args.spark_report:
        if args.check:
            parser.error('--spark-report refreshes source evidence and cannot be used with --check')
        report = json.loads(args.spark_report.read_text())
        if report.get('error') or len(report['etls']) != 25 or any(e['rows'] <= 0 for e in report['etls']):
            raise ValueError('Schema evidence requires 25 nonempty successful ETL outputs')
        if len(report['checks']) != 19 or any(str(row['实际值']) != '0' for row in report['checks']):
            raise ValueError('Schema evidence requires all 19 clean business checks to be zero')
        evidence = {key: report[key] for key in ('spark_version', 'as_of', 'seed', 'scenario', 'git_sha', 'worktree_dirty')}
        evidence['provenance'] = '本地合成小数据执行报告的schema提取；git_sha仅标识当时HEAD，worktree_dirty记录是否含未提交修改；每节点sql_sha256为文档SQL原文指纹，as_of确定执行时日期替换。提交后CI运行报告提供对应提交的验证。'
        evidence['etls'] = [{key: e[key] for key in ('output', 'path', 'rows', 'schema', 'nodes')} for e in report['etls']]
        evidence['public_sql'] = [{key: e[key] for key in ('output', 'path', 'rows', 'sql_sha256')} for e in report['public_sql']]
        outputs = build_contract(evidence=evidence)  # validate before changing the evidence source
        outputs['清单/Spark类型证据.json'] = json.dumps(evidence, ensure_ascii=False, indent=2)+'\n'
    else:
        outputs = build_contract()
    return write_or_check(outputs, args.check)


if __name__ == '__main__':
    raise SystemExit(main())
