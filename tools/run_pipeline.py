#!/usr/bin/env python3
"""Execute published ETL SQL verbatim on PySpark 3.4 local mode.

Only as_of_date substitutions are permitted. Input CSVs are never filtered or
repaired by this runner. Each completed output is a materialized checkpoint.
"""
from __future__ import annotations
import argparse
import hashlib
import csv
import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

@dataclass
class Node:
    id: str
    name: str
    kind: str
    sources: tuple[str,...]
    sql: str
    output: str|None = None

@dataclass
class Flow:
    path: Path
    output: str
    nodes: tuple[Node,...]
    inputs: tuple[str,...]


def topological_sort(dependencies):
    pending={key:set(value) for key,value in dependencies.items()};result=[]
    unknown=set().union(*pending.values())-set(pending) if pending else set()
    if unknown: raise ValueError(f'unknown dependencies: {sorted(unknown)}')
    while pending:
        ready=sorted(key for key,value in pending.items() if not value)
        if not ready:raise ValueError(f'dependency cycle / 环: {sorted(pending)}')
        for key in ready:
            result.append(key);del pending[key]
        for value in pending.values():value.difference_update(ready)
    return result


def input_aliases(sql):
    """Read actual table references, not comments, literals or column aliases."""
    import sqlglot
    from sqlglot import exp
    from sqlglot.optimizer.scope import traverse_scope
    aliases = set()
    for statement in sqlglot.parse(sql, read='spark'):
        if statement is None:
            raise ValueError('empty SQL statement')
        for scope in traverse_scope(statement):
            for _, (_, source) in scope.selected_sources.items():
                if not isinstance(source, exp.Table):
                    continue  # CTE/subquery definitions own their input references.
                match = re.fullmatch(r'input(\d*)', source.name, flags=re.I)
                if match:
                    suffix = match.group(1)
                    if suffix and (int(suffix) < 1 or str(int(suffix)) != suffix):
                        raise ValueError(f'invalid numbered input alias: {source.name}')
                    if source.db or source.catalog:
                        raise ValueError(f'input alias must be unqualified: {source.sql()}')
                    aliases.add(source.name.lower())
    return aliases


def input_ports(sql):
    return {1 if name == 'input' else int(name[5:]) for name in input_aliases(sql)}


def parse_document(text, path):
    sections=re.split(r'^### 节点\d+[^\n]*\n',text,flags=re.M)[1:]
    if not sections:
        mappings=re.findall(r'`input(\d+)\s*=\s*([^`]+)`',text)
        blocks=re.findall(r'```sql\s*\n(.*?)```',text,re.S)
        output=re.search(r'\*\*数据输出目标:\*\*\s*`([^`]+)`',text)
        if not mappings or len(blocks)!=1 or not output:raise ValueError(f'{path}: cannot parse single SQL mapping/output')
        indices=[int(i) for i,_ in mappings]
        if sorted(indices)!=list(range(1,len(indices)+1)):raise ValueError(f'{path}: non-contiguous input mappings')
        inputs=tuple(name.strip() for _,name in sorted(mappings,key=lambda pair:int(pair[0])))
        used=input_ports(blocks[0])
        if used and max(used)>len(inputs):raise ValueError(f'{path}: missing input{max(used)}')
        return Flow(path,output[1],(Node('sql','SQL','SQL_SCRIPT',inputs,blocks[0],output[1]),),inputs)
    nodes=[]
    for section in sections:
        values={key:re.search(r'^- '+key+r':\s*(.+)$',section,re.M) for key in ('Id','Name','Type','OutputDsName')}
        if any(values[key] is None for key in ('Id','Name','Type')):raise ValueError(f'{path}: missing node metadata')
        sql=re.search(r'^- 等价SQL:\s*\n```sql\s*\n(.*?)```',section,re.S|re.M)
        if sql is None:raise ValueError(f'{path}: missing equivalent SQL for {values["Id"][1]}')
        source_block=re.search(r'^- \*\*Sources \(Inputs\):\*\*\s*\n((?:  - [^\n]+\n)+)',section,re.M)
        sources=tuple(re.findall(r'^  - (\S+)',source_block[1],re.M)) if source_block else ()
        kind=values['Type'][1].strip();used=input_ports(sql[1])
        if kind=='INPUT_DATASET':
            unbound=input_aliases(sql[1])-{'input'}
            if unbound:raise ValueError(f'{path}: INPUT_DATASET only binds input, not {sorted(unbound)}')
        if kind!='INPUT_DATASET' and used and max(used)>len(sources):raise ValueError(f'{path}: missing input{max(used)} for {values["Id"][1]}')
        nodes.append(Node(values['Id'][1].strip(),values['Name'][1].strip(),kind,sources,sql[1],values['OutputDsName'][1].strip() if values['OutputDsName'] else None))
    by_id={node.id:node for node in nodes}
    if len(by_id)!=len(nodes):raise ValueError(f'{path}: duplicate node ID')
    order=topological_sort({node.id:set(node.sources) for node in nodes})
    outputs=[node.output for node in nodes if node.output]
    if len(outputs)!=1:raise ValueError(f'{path}: expected exactly one output')
    return Flow(path,outputs[0],tuple(by_id[key] for key in order),tuple(node.name for node in nodes if node.kind=='INPUT_DATASET'))


def parse_pipeline(root=ROOT):
    flows=[parse_document(path.read_text(),path) for path in sorted((Path(root)/'ETL/逻辑SQL').glob('*.md'))]
    by_output={flow.output:flow for flow in flows}
    if len(by_output)!=len(flows):raise ValueError('duplicate ETL output')
    order=topological_sort({flow.output:set(flow.inputs)&set(by_output) for flow in flows})
    return [by_output[name] for name in order]


def render_sql(sql, as_of):
    value=date.fromisoformat(str(as_of)).isoformat()
    return re.sub(r"DATE '2026-06-24'(?=\s+AS\s+`?as_of_date`?)",f"DATE '{value}'",sql.replace('${as_of_date}',value),flags=re.I)


def create_spark():
    from pyspark.sql import SparkSession
    os.environ.setdefault('SPARK_LOCAL_IP','127.0.0.1')
    spark=(SparkSession.builder.master('local[2]').appName('huiyuan-synthetic-verification')
           .config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2')
           .config('spark.sql.session.timeZone','Asia/Shanghai').config('spark.sql.adaptive.enabled','true').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    return spark


def register(frame, name):
    frame.createOrReplaceTempView('`'+name+'`')


def load_inputs(spark, fixture, root=ROOT):
    from pyspark.sql.types import StructType,StructField,StringType,LongType,DoubleType,DateType,TimestampType,BooleanType
    from tools.build_mini_world import field_types
    mapping={'STRING':StringType(),'LONG':LongType(),'INT':LongType(),'INTEGER':LongType(),'DOUBLE':DoubleType(),'FLOAT':DoubleType(),'DATE':DateType(),'TIMESTAMP':TimestampType(),'BOOLEAN':BooleanType()}
    result={}
    for path in sorted(Path(fixture).glob('*.csv')):
        if not path.stem.startswith(('dim_','dwd_','param_')):continue
        with path.open() as f:columns=next(csv.reader(f))
        types=field_types(path.stem)
        schema=StructType([StructField(column,mapping[types[column]],True) for column in columns])
        frame=(spark.read.option('mode','FAILFAST').option('header',True).option('nullValue','\\N').option('emptyValue','').option('timestampFormat','yyyy-MM-dd HH:mm:ss').schema(schema).csv(str(path))).localCheckpoint(eager=True)
        result[path.stem]=frame;register(frame,path.stem)
    return result


def run_pipeline(spark, fixture, as_of='2026-06-24', out=None, root=ROOT):
    root=Path(root);frames=load_inputs(spark,fixture,root);metrics=[]
    manifest_path=Path(fixture)/'manifest.json'
    manifest=json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    if out:Path(out).mkdir(parents=True,exist_ok=True)
    report={'spark_version':spark.version,'as_of':str(as_of),'seed':manifest.get('seed'),'scenario':manifest.get('scenario'),'git_sha':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),'worktree_dirty':bool(subprocess.check_output(['git','status','--porcelain'],cwd=root,text=True).strip()),'etls':metrics,'checks':[],'public_sql':[]}
    try:
        for flow in parse_pipeline(root):
            start=time.monotonic();nodes={};node_records=[]
            for node in flow.nodes:
                node_start=time.monotonic()
                try:
                    if node.kind=='INPUT_DATASET':
                        register(frames[node.name],'input')
                    else:
                        upstream=[nodes[key] for key in node.sources] if len(flow.nodes)>1 else [frames[key] for key in node.sources]
                        for i,frame in enumerate(upstream,1):register(frame,f'input{i}')
                        if upstream:register(upstream[0],'input')
                    frame=spark.sql(render_sql(node.sql,as_of))
                    # Force every selected column now. A later projection must not
                    # prune a runtime error from an intermediate SQL expression.
                    if node.kind != 'INPUT_DATASET':
                        frame=frame.localCheckpoint(eager=True)
                    nodes[node.id]=frame
                    node_records.append({'id':node.id,'name':node.name,'kind':node.kind,'sources':list(node.sources),'sql_sha256':hashlib.sha256(node.sql.encode()).hexdigest(),'materialization':'reused_input_checkpoint' if node.kind=='INPUT_DATASET' else 'eager_local_checkpoint','seconds':round(time.monotonic()-node_start,3)})
                except Exception as exc:raise RuntimeError(f'{flow.path.name}: node {node.id} ({node.name}) failed: {exc}') from exc
            output=next(nodes[node.id] for node in flow.nodes if node.output)
            output=output.localCheckpoint(eager=True);frames[flow.output]=output;register(output,flow.output)
            count=output.count();metrics.append({'output':flow.output,'path':str(flow.path.relative_to(root)),'rows':count,'columns':output.columns,'schema':output.schema.jsonValue(),'nodes_executed':len(node_records),'nodes':node_records,'seconds':round(time.monotonic()-start,3)})
            print(f'{flow.output}: {count} rows',flush=True)
            if out:output.coalesce(1).write.mode('overwrite').option('header',True).csv(str(Path(out)/flow.output))
        common=root/'ETL/公共口径'
        names={1:'bridge_触达订单归因',2:'bridge_券核销订单',3:'bridge_活动参与订单归因',5:'calendar_门店营业日历',6:'calendar_门店月份骨架',7:'scd2_门店时点关联',8:'bridge_规则任务生成'}
        for number in (1,2,3,8,5,6,7):
            start=time.monotonic()
            if number==7:
                facts=spark.sql('SELECT `门店ID`, `业务日期` AS `事实日期` FROM `dwd_订单` GROUP BY `门店ID`, `业务日期`');register(facts,'facts')
            path=next(common.glob(f'{number:02d}_*.sql'))
            frame=spark.sql(render_sql(path.read_text(),as_of)).localCheckpoint(eager=True)
            frames[names[number]]=frame;register(frame,names[number]);count=frame.count()
            report['public_sql'].append({'output':names[number],'path':str(path.relative_to(root)),'rows':count,'columns':frame.columns,'schema':frame.schema.jsonValue(),'sql_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'seconds':round(time.monotonic()-start,3)})
            if out:frame.coalesce(1).write.mode('overwrite').option('header',True).csv(str(Path(out)/names[number]))
        check=next(common.glob('04_*.sql'))
        report['checks']=[row.asDict() for row in spark.sql(render_sql(check.read_text(),as_of)).collect()]
    except Exception as exc:
        report['error']=str(exc);raise
    finally:
        if out:(Path(out)/'run-report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2,default=str)+'\n')
    return frames,report

if __name__=='__main__':
    import sys
    sys.path.insert(0,str(ROOT))
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--fixtures',type=Path,default=ROOT/'tests/fixtures/mini_world');p.add_argument('--as-of',default='2026-06-24');p.add_argument('--out',type=Path,required=True)
    args=p.parse_args();spark=create_spark()
    try:run_pipeline(spark,args.fixtures,args.as_of,args.out)
    finally:spark.stop()
