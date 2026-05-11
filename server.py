#!/usr/bin/env python3
"""
基金研究平台后端 (Python 版)
替代 server.js，提供完全相同的 API 接口

启动: python3 server.py
或:   ./start.sh
"""

import json
import re
import random
import calendar
import time
from datetime import date
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import requests
from flask import Flask, jsonify, send_from_directory, request, Response

# ── 可选 CORS 支持（同端口访问时不需要）────────────────────────
try:
    from flask_cors import CORS
    _has_cors = True
except ImportError:
    _has_cors = False

# ── 配置 ──────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
WEB_DIR  = BASE_DIR / 'web'       # 前端构建产物 (dist/)
DATA_DIR = WEB_DIR  / 'data'      # 基金 JSON 数据
PORT     = 3001                    # 离线独立版端口，Node版用3001

EM_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    ),
    'Referer': 'https://fundf10.eastmoney.com/',
}

STAGE_TITLE_MAP = {
    'Z': '近1周', 'Y': '近1月', '3Y': '近3月', '6Y': '近6月',
    'JN': '今年来', '1N': '近1年', '2N': '近2年', '3N': '近3年',
    '5N': '近5年', 'LN': '成立来',
}
STAGE_ORDER = ['Z', 'Y', '3Y', '6Y', 'JN', '1N', '2N', '3N', '5N', 'LN']

# ── Flask App ─────────────────────────────────────────────────────
app = Flask(__name__)
if _has_cors:
    CORS(app)


# ── 工具函数 ──────────────────────────────────────────────────────
def quartile_label(rank, total):
    if not rank or not total:
        return None
    pct = rank / total
    if pct <= 0.25: return '优秀'
    if pct <= 0.50: return '良好'
    if pct <= 0.75: return '一般'
    return '不佳'


def strip_tags(s):
    s = re.sub(r'<[^>]+>', ' ', s)
    s = s.replace('&nbsp;', ' ')
    return ' '.join(s.split()).strip()


def parse_trs(html):
    """从 HTML 中解析所有 <tr> 行，返回 [[td文本, ...], ...] 列表"""
    rows = []
    for tr_m in re.finditer(r'<tr[^>]*>([\s\S]*?)</tr>', html, re.IGNORECASE):
        tds = [strip_tags(td.group(1))
               for td in re.finditer(r'<td[^>]*>([\s\S]*?)</td>',
                                     tr_m.group(1), re.IGNORECASE)]
        if tds:
            rows.append(tds)
    return rows


def extract_content(raw):
    """从 fundf10 JSONP 响应中提取 content 字段的 HTML 字符串"""
    m = re.search(r'content\s*:\s*"((?:[^"\\]|\\.)*)"', raw)
    if not m:
        return ''
    try:
        return json.loads('"' + m.group(1) + '"')
    except Exception:
        return m.group(1)


def ok(data):
    return jsonify({'ok': True, 'data': data})


def err(msg, code=502):
    return jsonify({'ok': False, 'error': msg}), code


# ── 静态文件托管（SPA，hash 路由）────────────────────────────────
@app.route('/')
def index():
    return send_from_directory(str(WEB_DIR), 'index.html')


@app.route('/<path:path>')
def static_files(path):
    # API 路由由更具体的规则处理，这里只做安全兜底
    if path.startswith('api/'):
        return err('Not found', 404)
    full = WEB_DIR / path
    if full.exists() and full.is_file():
        return send_from_directory(str(WEB_DIR), path)
    # Vue hash 路由 fallback
    return send_from_directory(str(WEB_DIR), 'index.html')


# ── /api/fund/list ────────────────────────────────────────────────
@app.route('/api/fund/list')
def fund_list():
    try:
        data = json.loads((DATA_DIR / 'index.json').read_text(encoding='utf-8'))
        return ok(data)
    except Exception as e:
        return err(str(e), 500)


# ── /api/fund/:code ───────────────────────────────────────────────
@app.route('/api/fund/<code>')
def fund_detail(code):
    try:
        data = json.loads((DATA_DIR / f'{code}.json').read_text(encoding='utf-8'))
        return ok(data)
    except Exception:
        return err('基金数据未找到', 404)


# ── /api/fund/:code/realtime  实时净值（JSONP）────────────────────
@app.route('/api/fund/<code>/realtime')
def fund_realtime(code):
    try:
        url = f'http://fundgz.1234567.com.cn/js/{code}.js?rt={int(time.time()*1000)}'
        resp = requests.get(url, headers=EM_HEADERS, timeout=8)
        m = re.search(r'jsonpgz\((\{.*?\})\)', resp.text)
        if not m:
            raise ValueError('格式解析失败')
        p = json.loads(m.group(1))
        return ok({
            'code': p.get('fundcode'),
            'name': p.get('name'),
            'nav': float(p.get('dwjz', 0)),
            'navPrev': float(p.get('gsz', 0)),
            'growthRate': float(p.get('gszzl', 0)),
            'navDate': p.get('jzrq'),
            'updateTime': p.get('gztime'),
        })
    except Exception as e:
        return err(f'实时数据获取失败: {e}')


# ── /api/fund/:code/history  净值历史 ────────────────────────────
@app.route('/api/fund/<code>/history')
def fund_history(code):
    days = min(int(request.args.get('days', 90)), 365)
    try:
        url = (f'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNHisNetList'
               f'?pageIndex=1&pageSize={days}&plat=Android&appType=ttjj'
               f'&product=EFund&Version=1&deviceid=x&Fcode={code}&type=2')
        h = {**EM_HEADERS, 'Host': 'fundmobapi.eastmoney.com'}
        records = requests.get(url, headers=h, timeout=10).json().get('Datas', [])
        data = [{'date': r['FSRQ'], 'nav': float(r['DWJZ']),
                 'accNav': float(r['LJJZ']), 'growth': float(r['JZZZL'])}
                for r in records]
        data.reverse()
        return ok(data)
    except Exception as e:
        return err(f'历史数据获取失败: {e}')


# ── /api/fund/:code/performance  业绩表现 ────────────────────────
def _fetch_month_end(code, year, month):
    """抓取某年某月末的净值，返回 {'date':..., 'nav':...} 或 None"""
    last_day = calendar.monthrange(year, month)[1]
    url = (f'https://api.fund.eastmoney.com/f10/lsjz?fundCode={code}'
           f'&pageIndex=1&pageSize=20'
           f'&startDate={year}-{month:02d}-01&endDate={year}-{month:02d}-{last_day:02d}')
    try:
        h = {**EM_HEADERS, 'Host': 'api.fund.eastmoney.com'}
        lst = requests.get(url, headers=h, timeout=8).json().get('Data', {}).get('LSJZList', [])
        return {'date': lst[0]['FSRQ'], 'nav': float(lst[0]['DWJZ'])} if lst else None
    except Exception:
        return None


def _fetch_latest_nav(code):
    """抓取最新净值"""
    url = (f'https://api.fund.eastmoney.com/f10/lsjz'
           f'?fundCode={code}&pageIndex=1&pageSize=5&startDate=&endDate=')
    try:
        h = {**EM_HEADERS, 'Host': 'api.fund.eastmoney.com'}
        lst = requests.get(url, headers=h, timeout=8).json().get('Data', {}).get('LSJZList', [])
        return {'date': lst[0]['FSRQ'], 'nav': float(lst[0]['DWJZ'])} if lst else None
    except Exception:
        return None


@app.route('/api/fund/<code>/performance')
def fund_performance(code):
    today = date.today()
    this_year, this_month = today.year, today.month
    start_year = 2018
    h_mob = {**EM_HEADERS, 'Host': 'fundmobapi.eastmoney.com'}
    stage_url = (f'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNPeriodIncrease'
                 f'?FCODE={code}&deviceid=x&plat=Android&product=EFund&version=1')

    # 构建季末净值抓取任务列表
    tasks = []  # [(nav_map_key, callable), ...]
    for y in range(start_year - 1, this_year + 1):
        months = ([3, 6, 9, 12] if y < this_year
                  else [m for m in [3, 6, 9] if m <= this_month])
        for m in months:
            tasks.append((f'{y}-{m}',
                          lambda _c=code, _y=y, _m=m: _fetch_month_end(_c, _y, _m)))
        if y == this_year:
            tasks.append((f'{y}-latest', lambda _c=code: _fetch_latest_nav(_c)))

    try:
        nav_map = {}
        with ThreadPoolExecutor(max_workers=20) as ex:
            stage_fut = ex.submit(requests.get, stage_url,
                                  **{'headers': h_mob, 'timeout': 10})
            nav_futs = [(ex.submit(fn), key) for key, fn in tasks]

            stage_resp = stage_fut.result()
            for fut, key in nav_futs:
                result = fut.result()
                if result:
                    nav_map[key] = result['nav']

        # 阶段涨幅
        stage_map = {s['title']: s for s in stage_resp.json().get('Datas', [])}
        stages = []
        for k in STAGE_ORDER:
            s = stage_map.get(k)
            if not s or s.get('syl') is None:
                continue
            rank  = int(s['rank']) if s.get('rank') else None
            total = int(s['sc'])   if s.get('sc')   else None
            stages.append({
                'period':    STAGE_TITLE_MAP[k],
                'fund':      float(s['syl']),
                'peer':      float(s['avg'])   if s.get('avg')   else None,
                'hs300':     float(s['hs300']) if s.get('hs300') else None,
                'rank':      rank,
                'rankTotal': total,
                'quartile':  quartile_label(rank, total),
            })

        # 年度涨幅（按年末净值计算）
        annual = []
        for y in range(start_year, this_year + 1):
            prev_nav = nav_map.get(f'{y-1}-12')
            curr_nav = (nav_map.get(f'{y}-12') if y < this_year
                        else nav_map.get(f'{this_year}-latest'))
            if prev_nav and curr_nav:
                ret = round((curr_nav - prev_nav) / prev_nav * 100, 2)
                entry = {'year': y, 'fund': ret}
                if y == this_year:
                    entry['partial'] = True
                annual.append(entry)

        # 季度涨幅（按季末净值计算）
        q_months = [3, 6, 9, 12]
        quarterly = []
        for y in range(start_year, this_year + 1):
            for qi, m in enumerate(q_months):
                if y == this_year and m > this_month:
                    continue
                curr_nav = nav_map.get(f'{y}-{m}')
                prev_m   = 12 if qi == 0 else q_months[qi - 1]
                prev_y   = y - 1 if qi == 0 else y
                prev_nav = nav_map.get(f'{prev_y}-{prev_m}')
                if curr_nav and prev_nav:
                    ret = round((curr_nav - prev_nav) / prev_nav * 100, 2)
                    quarterly.append({'quarter': f'{y}Q{qi+1}', 'fund': ret})

        return ok({'stages': stages, 'annual': annual, 'quarterly': quarterly})
    except Exception as e:
        return err(f'业绩数据获取失败: {e}')


# ── /api/fund/:code/holdings  持仓结构 ───────────────────────────
@app.route('/api/fund/<code>/holdings')
def fund_holdings(code):
    h_f10 = {**EM_HEADERS, 'Host': 'fundf10.eastmoney.com'}
    h_mob = {**EM_HEADERS, 'Host': 'fundmobapi.eastmoney.com'}
    jjcc_url = (f'https://fundf10.eastmoney.com/FundArchivesDatas.aspx'
                f'?type=jjcc&code={code}&topline=20&year=&month=&rt={random.random()}')
    alloc_url = (f'https://fundmobapi.eastmoney.com/FundMNewApi/FundMNAssetAllocation'
                 f'?FCODE={code}&deviceid=x&plat=Android&product=EFund&version=1')
    try:
        with ThreadPoolExecutor(max_workers=2) as ex:
            jjcc_fut  = ex.submit(requests.get, jjcc_url,  **{'headers': h_f10, 'timeout': 10})
            alloc_fut = ex.submit(requests.get, alloc_url, **{'headers': h_mob, 'timeout': 10})
            jjcc_resp  = jjcc_fut.result()
            alloc_resp = alloc_fut.result()

        content = extract_content(jjcc_resp.text)

        # 报告期日期
        date_m = re.search(r'<font[^>]*>(\d{4}-\d{2}-\d{2})</font>', content)
        report_date = date_m.group(1) if date_m else None

        # 前十大持仓
        # 列：序号(0)|代码(1)|名称(2)|最新价(3,空)|涨跌幅(4,空)|相关资讯(5)|占净值比例(6)|持股数(7)|市值(8)
        top10 = []
        for tds in parse_trs(content):
            if len(tds) <= 6:
                continue
            try:
                rank = int(tds[0].rstrip('*'))
            except Exception:
                continue
            if rank < 1 or rank > 20:
                continue
            code_m = re.search(r'\d{6}', tds[1])
            if not code_m:
                continue
            ratio_m = re.search(r'([\d.]+)%', tds[6])
            if not ratio_m:
                continue
            top10.append({
                'rank':  rank,
                'code':  code_m.group(),
                'name':  tds[2].split()[0] if tds[2] else '',
                'ratio': float(ratio_m.group(1)),
            })

        top10_total = round(sum(h['ratio'] for h in top10), 2)

        # 仓位比例（FundMNAssetAllocation）
        alloc_datas = alloc_resp.json().get('Datas', [])
        latest = alloc_datas[0] if alloc_datas else None

        def safe_float(val):
            return float(val) if val and val != '--' else None

        return ok({
            'top10':       top10,
            'top10Total':  top10_total,
            'stockRatio':  safe_float(latest.get('GP')) if latest else None,
            'cashRatio':   safe_float(latest.get('HB')) if latest else None,
            'date':        latest['FSRQ'] if latest else report_date,
        })
    except Exception as e:
        return err(f'持仓数据获取失败: {e}')


# ── /api/manager/:id  基金经理历任基金 ───────────────────────────
@app.route('/api/manager/<manager_id>')
def manager_detail(manager_id):
    h = {
        'User-Agent': (
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ),
        'Accept':  'text/html,application/xhtml+xml',
        'Referer': 'https://fund.eastmoney.com/',
    }
    try:
        resp = requests.get(f'https://fund.eastmoney.com/manager/{manager_id}.html',
                            headers=h, timeout=12)
        html = resp.text
        funds, seen = [], set()

        for tds in parse_trs(html):
            if not tds or not re.match(r'^\d{6}$', tds[0]):
                continue
            fund_code = tds[0]
            name = tds[1].split()[0] if len(tds) > 1 else ''
            rest = ' '.join(tds[2:])

            date_m = re.search(r'(\d{4}-\d{2}-\d{2})\s*~\s*(\d{4}-\d{2}-\d{2}|至今)', rest)
            if not date_m:
                continue
            start, end = date_m.group(1), date_m.group(2)
            is_current = (end == '至今')

            ret_m = re.search(r'([-\d.]+)%\s*$', rest)
            if not ret_m:
                continue
            tenure_return = float(ret_m.group(1))

            days_m = re.search(r'(\d+)年又(\d+)天|(\d+)天|(\d+)年', rest)
            days = 0
            if days_m:
                if days_m.group(1):
                    days = int(days_m.group(1)) * 365 + int(days_m.group(2))
                elif days_m.group(3):
                    days = int(days_m.group(3))
                elif days_m.group(4):
                    days = int(days_m.group(4)) * 365

            type_m = re.search(r'(混合型|股票型|债券型|指数型|QDII|货币型)[^\s]*', rest)
            fund_type = type_m.group(0) if type_m else ''

            key = f'{fund_code}-{start}'
            if key in seen:
                continue
            seen.add(key)
            funds.append({
                'code': fund_code, 'name': name, 'type': fund_type,
                'start': start, 'end': end, 'isCurrent': is_current,
                'days': days, 'tenureReturn': tenure_return,
            })

        return ok({'managerId': manager_id, 'funds': funds})
    except Exception as e:
        return err(f'经理数据获取失败: {e}')


# ── 启动 ──────────────────────────────────────────────────────────
if __name__ == '__main__':
    if not WEB_DIR.exists():
        print(f'⚠️  web/ 目录不存在，请先运行: cp -r ../web-platform/dist web/')
    print(f'\n🚀 基金研究平台 (Python 版) 运行中')
    print(f'   页面: http://localhost:{PORT}')
    print(f'   API:  http://localhost:{PORT}/api/\n')
    app.run(host='0.0.0.0', port=PORT, debug=False, threaded=True)
