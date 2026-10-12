#!/usr/bin/env python3
"""Deterministic, joined synthetic input for the documented Spark pipeline.

Clean and adversarial scenarios are separate; no executor-side input cleaning.
"""
from __future__ import annotations
import argparse
import calendar
import csv
import json
import random
import re
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / 'tests/fixtures/mini_world'
NULL = '\\N'  # preserves the distinction between SQL NULL and empty string


def headers(table):
    with (ROOT/'数据集/数据样本'/f'{table}.csv').open(encoding='utf-8-sig') as f:
        return next(csv.reader(f))


def field_types(table):
    text = (ROOT/'数据集/结构定义'/f'{table}.md').read_text()
    result = dict(re.findall(r'^- (.+?) \(fdId:[^\n]*\n  - 字段类型: (\w+)', text, re.M))
    # v1.4.1 columns added to the published CSV contract without historical metadata.
    for field in headers(table):
        if field not in result:
            if field.endswith(('时间',)):
                result[field] = 'TIMESTAMP'
            elif field.endswith(('日期', '起始日')):
                result[field] = 'DATE'
            elif field.startswith('是否') or field.endswith(('标记', '天数上限', '预警月数')):
                result[field] = 'LONG'
            elif field.endswith(('金额', '率', '占比上限', '占比下限', '阈值pp', '月降幅预警')):
                result[field] = 'DOUBLE'
            else:
                result[field] = 'STRING'
    # Historical export says LONG, but the published masked display is a string.
    # Keep the historical document; do not manufacture or unmask a numeric phone.
    if table == 'dim_会员主档':
        result['手机号'] = 'STRING'
    return result


def month_shift(value, n):
    y, m = divmod(value.year*12+value.month-1+n, 12)
    return date(y, m+1, 1)


def build_world(out=DEFAULT_OUT, seed=41, scale='small', as_of='2026-06-24', scenario='clean'):
    if scale not in ('small', 'full') or scenario not in ('clean', 'adversarial'):
        raise ValueError('unknown scale or scenario')
    as_of = date.fromisoformat(str(as_of))
    out = Path(out); out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    tables = {p.stem: [] for p in sorted((ROOT/'数据集/数据样本').glob('*.csv'))
              if p.stem.startswith(('dim_', 'dwd_', 'param_'))}
    types = {t: field_types(t) for t in tables}
    def add(table, **values):
        row = {k: (0 if v in ('LONG','DOUBLE','INTEGER','INT','FLOAT') else
                   NULL if v in ('DATE','TIMESTAMP') else '') for k,v in types[table].items()}
        row.update(values); tables[table].append(row); return row
    levels = ('普卡','银卡','金卡','黑卡')
    store_types = ('商场店','社区店','写字楼店','交通店','学校店','夜市店','外卖卫星店','旗舰店','快取店')
    # Parameter values are the repository's explicitly simulated policy examples.
    for table in (t for t in tables if t.startswith('param_')):
        with (ROOT/'数据集/数据样本'/f'{table}.csv').open() as f:
            for row in csv.DictReader(f):
                row = {k:(NULL if not v and types[table][k]=='DATE' else v) for k,v in row.items()}
                if table == 'param_会员生命周期阈值': row['生效日期'] = '2025-01-01'
                tables[table].append(row)
    nmember, nstore, norder = (300,12,1800) if scale=='small' else (80000,1200,1294316)
    stores=[]
    for i in range(nstore):
        sid=f'S9000{i+1:02d}' if i<12 else f'S{i+1:06d}'
        opened = month_shift(as_of,-11) if i != 11 else as_of-timedelta(days=44)
        closed = (as_of-timedelta(days=40)).isoformat() if i==10 else ''
        row=add('dim_门店主档', 门店ID=sid, 门店版本ID=sid+'V1', 生效起始日期=opened,
                当前版本标记=1, 门店名称=f'演示{i+1}店', 门店编码=sid, POS门店编码=sid,
                财务门店编码=sid, 外卖平台门店编码=sid, 品牌线='演示品牌',直营加盟类型='加盟' if i<6 else '直营',
                省份='演示省', 城市='演示市',区域='演示区',店型=store_types[i%9],门店类型=store_types[i%9],
                商圈='演示商圈',城市层级='二线',面积_平方米=120,座位数=30,到店能力等级='高',
                开业日期=opened,闭店日期=closed,营业状态='闭店' if closed else '营业',
                是否90天内新店='是' if i==11 else '否',新店标签='新店' if i==11 else '非新店',店长姓名='演示店长',区域经理='演示经理')
        stores.append(row)
    for i in range(3 if scale=='small' else 141):
        add('dim_加盟商主档',加盟商ID=f'F{i+1:03d}',加盟商名称=f'演示加盟商{i+1}',加盟商类型='个人',联系人='演示联系人',手机号Hash='synthetic_hash',签约省份='演示省',入网日期=month_shift(as_of,-12),合作状态='正常',信用等级='A',保证金金额=1000,门店数量=2)
    for i in range(20 if scale=='small' else 74):
        add('dim_商品主档',商品ID=f'P{i+1:03d}',商品版本ID=f'PV{i+1:03d}',商品名称=f'演示商品{i+1}',商品编码=f'P{i+1:03d}',一级类目='餐品',二级类目='主食',售价=30,成本=10,上架日期=month_shift(as_of,-12),是否季节限定=0,是否在售=1)
    for i in range(4):
        add('dim_券模板',券模板ID=f'CT{i+1}',券名称=f'演示券{i+1}',券类型='优惠券',优惠形式='满减',面值=10,使用门槛=20,折扣率=0.9,适用品类='全部',是否有效=1)
        add('dim_活动主档',活动ID=f'A{i+1}',活动名称=f'演示活动{i+1}',活动类型='促销',活动渠道='企微',目标人群标签='全部',开始日期=month_shift(as_of,-12),结束日期=as_of,目标人群等级='全部',预算=1000)
    for i,store in enumerate(stores):
        add('dim_员工导购',员工ID=f'E{i+1:04d}',姓名=f'演示员工{i+1}',归属门店ID=store['门店ID'],岗位='导购',角色标签='店员',入职日期=store['开业日期'],状态='在职',员工等级='初级')
    members=[]
    for i in range(nmember):
        mid=f'M{i+1:06d}'
        registered=month_shift(as_of,-11)+timedelta(days=rng.randrange(290))
        if registered>as_of: registered=as_of
        members.append(add('dim_会员主档',会员ID=mid,注册日期=registered,注册门店ID=stores[i%nstore]['门店ID'],注册渠道='企微',会员等级=levels[i%4],性别='未知',年龄段='30-39',生日='1990-01-01',城市='演示市',手机号='156****0925',会员状态='正常',最近活跃日期=registered,等级升级日期=registered))
    edge_ids=['M900001','M900002','M900003','M900004','M900010','M900020']
    for mid in edge_ids:
        members.append(add('dim_会员主档',会员ID=mid,注册日期=as_of-timedelta(days=200 if mid!='M900003' else 2),注册门店ID='S900001',注册渠道='企微',会员等级='普卡',性别='未知',年龄段='30-39',生日='1990-01-01',城市='演示市',手机号='156****0925',会员状态='正常'))
    def order(oid,mid,sid,when,amount=100,status='已完成',takeaway=0):
        if isinstance(when,date) and not isinstance(when,datetime): when=datetime.combine(when,datetime.min.time()).replace(hour=12)
        row=add('dwd_订单',订单ID=oid,门店ID=sid,会员ID=mid,顾客识别键='synthetic_'+str(mid),身份匹配置信度=1,下单时间=when.isoformat(' '),业务日期=when.date(),原价金额=amount+10,折扣金额=10,实付金额=amount,收入口径金额=amount,支付方式='微信',销售渠道='美团' if takeaway else '到店',履约方式='外卖' if takeaway else '堂食',是否到店=1-takeaway,订单状态=status,是否用券=0,是否会员首单=0,商品件数=1)
        add('dwd_订单商品',订单ID=oid,商品序号=1,商品ID=f'P{1+rng.randrange(20):03d}',数量=1,单价=amount,行金额=amount,是否主商品=1)
        return row
    for i in range(norder):
        m=members[i%nmember]; store=stores[i%nstore]
        # S900009 intentionally has no sales, but its full cost skeleton remains.
        if store['门店ID']=='S900009': store=stores[0]
        start=max(date.fromisoformat(str(m['注册日期'])),date.fromisoformat(str(store['开业日期'])))
        end=min(as_of,date.fromisoformat(store['闭店日期']) if store['闭店日期'] else as_of)
        if start>end: store=stores[0];start=date.fromisoformat(str(m['注册日期']));end=as_of
        when=start+timedelta(days=rng.randrange((end-start).days+1))
        order(f'O{i+1:07d}',m['会员ID'],store['门店ID'],when,rng.choice((30,50,100,200)),takeaway=i%2)
    anchor=datetime.combine(as_of-timedelta(days=12),datetime.min.time()).replace(hour=12)
    order('O900001','M900001','S900001',anchor+timedelta(days=7,hours=23,minutes=59,seconds=59))
    order('O900002','M900001','S900001',anchor+timedelta(days=8))
    order('O900003','M900002','S900001',as_of-timedelta(days=100))
    order('O900010','M900010','S900001',as_of-timedelta(days=100))
    for i,mid in enumerate((NULL,'','null')): order(f'O90000{i+4}',mid,'S900001',as_of-timedelta(days=10))
    order('O900007','M900001','S900001',as_of-timedelta(days=1),status='已取消')
    order('O900008','M900001','S900001',as_of-timedelta(days=1),status='已退款')
    def touch(tid,mid,when):
        add('dwd_会员触达',触达ID=tid,会员ID=mid,触达时间=when,触达日期=str(when)[:10],员工ID='E0001',触达渠道='企微',内容类型='活动',内容ID='C1',活动ID='A1',是否查看=1,触达状态='已发送')
    touch('T900001','M900001',anchor-timedelta(hours=1))
    touch('T900002','M900001',anchor);touch('T900003','M900001',anchor)
    touch('T900004','M900004',datetime.combine(as_of-timedelta(days=1),datetime.min.time()))
    touch('T900020','M900020',datetime.combine(as_of-timedelta(days=1),datetime.min.time()))
    for i,row in enumerate(tables['dwd_订单'][:250 if scale=='small' else 500000]):
        when=datetime.fromisoformat(str(row['下单时间']))-timedelta(hours=1)
        touch(f'T{i+1:07d}',row['会员ID'],when)
        add('dwd_活动参与',参与ID=f'AP{i+1:07d}',会员ID=row['会员ID'],活动ID=f'A{1+i%4}',参与时间=when,参与日期=when.date(),行为类型='报名',结果='成功')
        if i%2==0:
            add('dwd_券事件',券ID=f'C{i+1:07d}',券模板ID=f'CT{1+i%4}',来源活动ID=f'A{1+i%4}',会员ID=row['会员ID'],发放日期=when.date(),失效日期=when.date()+timedelta(days=7),核销日期=row['业务日期'],门店ID=row['门店ID'],订单ID=row['订单ID'],折扣金额=10,发放渠道='企微')
    for cid,discount,expiry in [('C900001',10,as_of),('C900002',20,as_of),('C900003',30,as_of-timedelta(days=10))]:
        add('dwd_券事件',券ID=cid,券模板ID='CT1',来源活动ID='A1',会员ID='M900001',发放日期=anchor.date(),失效日期=expiry,核销日期=anchor.date()+timedelta(days=7),门店ID='S900001',订单ID='O900001',折扣金额=discount,发放渠道='企微')
    add('dwd_券事件',券ID='C900004',券模板ID='CT1',来源活动ID='A1',会员ID='M900002',发放日期=as_of-timedelta(days=3),失效日期=as_of+timedelta(days=2),门店ID='S900001',折扣金额=10,发放渠道='企微')
    for mid in ('M900004','M900020'):
        add('dwd_评价',评价ID='R'+mid,订单ID='O900001',会员ID=mid,门店ID='S900001',评分=1,评价时间=datetime.combine(as_of-timedelta(days=1),datetime.min.time()),评价日期=as_of-timedelta(days=1),评价平台='演示平台',内容摘要Hash='synthetic',负评标签='服务',回复状态='未回复')
    add('dwd_评价',评价ID='R_EARLY',订单ID='O900003',会员ID='M900002',门店ID='S900001',评分=2,评价时间=as_of-timedelta(days=30),评价日期=as_of-timedelta(days=30),评价平台='演示平台',负评标签='服务',回复状态='未回复')
    add('dwd_会员经营任务',任务ID='TASK_EARLY',会员ID='M900002',归属门店ID='S900001',员工导购ID='E0001',任务来源='人工',任务优先级='P0',任务类型='流失预警',任务生成时间=as_of-timedelta(days=40),任务截止时间=as_of,任务失效时间=as_of,触达状态='未触达',任务结果='待处理')
    add('dwd_投诉',投诉ID='COM1',会员ID='M900004',订单ID='O900001',门店ID='S900001',投诉类型='服务',投诉时间=datetime.combine(as_of-timedelta(days=1),datetime.min.time()),状态='待处理',处理人ID='E0001',处理时长_小时=0)
    add('dwd_会员经营任务',任务ID='TASK1',会员ID='M900004',归属门店ID='S900001',员工导购ID='E0001',任务来源='人工',任务优先级='P1',任务类型='新会员首单',人群标签='新客',推荐动作='回访',推荐权益='演示券',推荐原因='模拟任务',预计价值=100,任务生成时间=as_of-timedelta(days=1),任务截止时间=as_of,任务失效时间=as_of,触达状态='未触达',触达方式='企微',触达后下单=0,任务结果='待处理')
    subjects=[('CST_RAW','原材料','变动成本'),('CST_PKG','包材','变动成本'),('CST_PLAT','平台抽佣','变动成本'),('CST_LBR','人工','半固定成本'),('CST_RENT','房租','固定成本'),('CST_UTL','能耗','半固定成本'),('CST_DEP','折旧','固定成本'),('CST_HQ','总部','固定成本')]
    for code,name,cat in subjects:
        add('dim_成本科目',成本科目ID=code,成本科目名称=name,成本大类=cat,成本性质=cat,默认营收占比=0.1,是否按店型差异化=1,适用场景='全部')
    for i,s in enumerate(stores):
        start=date.fromisoformat(str(s['开业日期']));end=min(as_of,date.fromisoformat(s['闭店日期']) if s['闭店日期'] else as_of)
        for n in range(12):
            month=month_shift(as_of,-11+n)
            if month<start.replace(day=1) or month>end.replace(day=1):continue
            # Known store loss series: Jan-Feb loss, March profit, Apr-May-Jun loss.
            sales=sum(float(o['实付金额']) for o in tables['dwd_订单'] if o['门店ID']==s['门店ID'] and str(o['业务日期'])[:7]==month.isoformat()[:7] and o['订单状态']=='已完成') if scale=='small' else 100000
            budget=0 if i==0 and month==month_shift(as_of,-3) else (sales+100 if i==0 else (5000 if i==8 else 100))
            for j,(code,name,cat) in enumerate(subjects):
                add('dwd_门店成本明细',成本明细ID=f'COST{i:04d}{n:02d}{j}',门店ID=s['门店ID'],月份=month,成本科目ID=code,成本科目名称=name,成本大类=cat,成本金额=(budget if j==0 else 0) if i==0 else budget/8,营收基数=sales,成本占比=0.1,数据来源='模拟',数据状态='已确认',录入日期=min(as_of,month+timedelta(days=20)))
            add('dwd_门店目标',目标ID=f'GOAL{i:04d}{n:02d}',门店ID=s['门店ID'],年月=month,目标指标='销售额',目标值=10000)
        if i<6:
            fid=f'F{1+i%3:03d}'
            add('dwd_加盟合同明细',合同ID=f'CON{i}',加盟商ID=fid,门店ID=s['门店ID'],门店类型=s['门店类型'],签约日期=start,合同期年数=3,到期日=month_shift(as_of,36),分成模型='流水抽成',流水抽成率=0.05,年加盟费=1200,装修标准='演示',装修投入金额=100,设备投入金额=100,加盟费金额=100,保证金金额=100,总投资额=400,续约状态='未到期',合同状态='有效')
            add('dwd_门店投资明细',投资明细ID=f'INV{i}',门店ID=s['门店ID'],加盟商ID=fid,投资项='装修',投资金额=1 if i==1 else 100000,投资日期=start+timedelta(days=14),投资类型='初始投资',回收性质='不可回收')
            add('dwd_加盟分账明细',分账明细ID=f'SET{i}',加盟商ID=fid,门店ID=s['门店ID'],合同ID=f'CON{i}',月份=as_of.replace(day=1),月营收基数=10000,流水抽成金额=500,原料供应链利润=100,年加盟费分摊=100,督导费用=100,总部当月收入=700,总部当月净收益=600,对账状态='已确认',结算日期=as_of)
    for i,m in enumerate(members):
        add('dim_会员身份桥',身份桥ID=f'ID{i}',会员ID=m['会员ID'],会员卡号='synthetic_'+m['会员ID'],手机号Hash='synthetic_hash',身份类型='会员卡',绑定状态='已绑定',首次出现时间=m['注册日期'],最近出现时间=as_of,匹配置信度=1,匹配方式='模拟精确')
        if i<20:
            add('dwd_会员注册绑定',事件ID=f'REG{i}',会员ID=m['会员ID'],事件类型='注册',事件时间=m['注册日期'],事件日期=m['注册日期'],门店ID=m['注册门店ID'],渠道='企微',结果='成功')
            add('dwd_积分事件',流水ID=f'POINT{i}',会员ID=m['会员ID'],交易日期=as_of,交易时间=as_of,流水类型='消费',积分变化=10,订单ID=f'O{i+1:07d}',备注='模拟')
            add('dwd_私域事件',事件ID=f'PRI{i}',会员ID=m['会员ID'],事件时间=as_of,事件日期=as_of,事件类型='加微',员工ID='E0001',门店ID=m['注册门店ID'],来源渠道='企微',标签='模拟',状态='成功')
    for n in range(366):
        day=as_of-timedelta(days=365-n)
        add('dim_日期',日期=day,年份=day.year,月份=day.month,日=day.day,周=day.isocalendar().week,季度=(day.month-1)//3+1,年月=day.replace(day=1),星期=day.weekday()+1,星期名称='星期'+str(day.weekday()+1),是否周末=int(day.weekday()>4),是否节假日=0,节假日名称='',营销周期标签='常规')
    if scenario=='adversarial':
        overlap=dict(stores[0]);overlap.update(门店版本ID='S900001V2',生效起始日期=as_of-timedelta(days=20),当前版本标记=0,门店名称='演示重叠版本');tables['dim_门店主档'].append(overlap)
        future=as_of+timedelta(days=1)
        order('O999999','M900001','S900001',future)
        touch('T999999','M900001',datetime.combine(future,datetime.min.time()))
        add('dwd_活动参与',参与ID='AP999999',会员ID='M900001',活动ID='A1',参与时间=future,参与日期=future,行为类型='报名',结果='成功')
        add('dwd_券事件',券ID='C999999',券模板ID='CT1',会员ID='M900001',发放日期=as_of,失效日期=future,核销日期=future,门店ID='S900001',订单ID='O999999',折扣金额=10)
    for table, rows in tables.items():
        with (out/f'{table}.csv').open('w',encoding='utf-8',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=headers(table),quoting=csv.QUOTE_ALL);writer.writeheader()
            for row in rows:
                normalized=dict(row)
                for key,value in normalized.items():
                    if types[table][key]=='TIMESTAMP' and value != NULL:
                        if isinstance(value,date) and not isinstance(value,datetime):
                            normalized[key]=datetime.combine(value,datetime.min.time()).isoformat(' ')
                        elif isinstance(value,datetime):normalized[key]=value.isoformat(' ')
                writer.writerow(normalized)
    expected=[
        ('O900001','bridge_触达订单归因','订单ID','触达ID','T900003','同时间戳触达 ID 降序；第7天末仍在8日开区间内'),
        ('O900002','bridge_触达订单归因','订单ID','__row_count__','0','恰第8天初不归因'),
        ('O900001','bridge_券核销订单','订单ID','券ID','C900002','有效两券金额20胜10；已过期金额30排除'),
        ('M900003','dws_会员生命周期','会员ID','生命周期阶段','新客-未首单','注册2天且0单，活跃阈值30天'),
        ('M900002','dws_会员生命周期','会员ID','总订单数','1','仅埋点已完成订单1单'),
        ('M900002','dws_会员RFM分层','会员ID','RFM标签','一般挽留客户','仅1单100元且末单100天：R=1 F=1 M=2'),
        ('M900002','dws_会员生命周期','会员ID','生命周期阶段','流失预警','末单100天，60<100<=180'),
        ('M900002','bridge_规则任务生成','会员ID','任务优先级','P0','同时命中流失预警和即将到期券，P0优先'),
        ('M900010','bridge_规则任务生成','会员ID','是否对照组','1','数字尾号0且营销任务进入holdout'),
        ('M900020','bridge_规则任务生成','会员ID','是否对照组','0','负评修复豁免holdout且豁免近7日触达'),
        ('M900020','bridge_规则任务生成','会员ID','任务类型','负评修复','未回复低分评价在3日内'),
        ('S900009|'+as_of.strftime('%Y-%m'),'dws_单店利润月汇总','门店ID|月份','月营收','0','该店不生成销售，成本月份保留'),
        ('S900001|'+month_shift(as_of,-3).strftime('%Y-%m'),'ads_单店利润健康','门店ID|月份','连续亏损月数','0','快照前三月销售正数且成本零，盈利重置'),
        ('S900001|'+as_of.strftime('%Y-%m'),'ads_单店利润健康','门店ID|月份','连续亏损月数','3','四五月六月每月变动成本比销售多100'),
    ]
    with (out/'expected.csv').open('w',newline='') as f:
        w=csv.writer(f);w.writerow(('埋点对象','表','键字段','字段','期望值','手算说明'));w.writerows(expected)
    (out/'manifest.json').write_text(json.dumps({'seed':seed,'scale':scale,'as_of':str(as_of),'scenario':scenario,'counts':{k:len(v) for k,v in tables.items()}},ensure_ascii=False,indent=2)+'\n')
    (out/'EDGE_CASES.md').write_text('''# 合成边界案例

默认 clean 是跨表关联的合法输入；adversarial 是显式异常输入。执行器不清洗输入。

O900001：并列触达 T900003 胜 T900002，7天末归因；O900002：8天初不归因。对应三桥差分与 known answers，验收10/13。
O900001：多券按金额取 C900002；过期 C900003 排除。对应验收11/14。
O900004/5/6：SQL NULL（CSV \\N）、空串、字面量 null 分开保留；literal null 仍按现有规则处理，不擅改会员含义。
O900007/8：已取消/已退款排除，桥非空并逐键对账。
S900009：零销售有成本，利润骨架保留；所有门店有零订单营业日。对应17和时间参照。
S900011：闭店；S900012：90天内新店；新店输出保留零单日。对应23/26。
同期群：当前月及未来观察月份未完成时 Mn 人数/率为空（M0除外）。对应16/25与同期群参照。
S900001：一二月亏损、三月盈利、四五月六月亏损，六月连续亏损3。对应18和已知答案。
加盟门店：小投资已回本、大投资未回本、投资月中开始。对应19和回本状态范围。
M900002：流失预警P0和待核销券P2同时命中，P0胜；M900004：已触达仍保留负评修复；M900010：营销holdout；M900020：负评修复既豁免防打扰也不进holdout。对应28、任务差分和已知答案。

adversarial 追加 S900001V2 重叠门店版本，07应选起始日更晚版本并保持门店日粒度，原始验收20必须检出重叠；追加 O/T/AP/C999999 未来事实，所有加工输出应排除，原始验收21必须非零。异常数不得压成零。
月份断点：Spark 测试把已验证贡献利润四/六月作为独立输入（显式移除五月），原样运行文档连续亏损节点，六月必须重置为1；默认完整营业月份骨架不会人为漏掉月份。
''')
    return tables

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--seed',type=int,default=41);p.add_argument('--scale',choices=('small','full'),default='small');p.add_argument('--as-of',default='2026-06-24');p.add_argument('--scenario',choices=('clean','adversarial'),default='clean');p.add_argument('--out',type=Path,default=DEFAULT_OUT)
    a=p.parse_args();build_world(a.out,a.seed,a.scale,a.as_of,a.scenario)
